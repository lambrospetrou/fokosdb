# RFC — `graphGetItems`: a read-only graph of dependent reads

**State:** Draft
**Date:** 2026-11-27
**Author:** Lambros Petrou
**Status:** Nothing is built.

References:

- `docs/agent-plans/2026-10-17-batch-get-and-write-items.md`
- `docs/agent-plans/2026-11-01-routing-hash.md`
- `src/client/db.ts`: `#readTransaction`, `#readSnapshotFastPath`, `withMetaSoFar`
- `src/server/do-partition.ts`: `apiBatchGetItems`, `txReadSnapshot`, `txReadForTransaction`
- `src/sharding/runtime.ts`: `FokosShardingRuntime.ownerCheck`
- `src/sharding/client.ts`: `FokosShardingClient.resolveAll`
- `src/shared/transaction-limits.ts`: `MAX_PAYLOAD_BYTES_PER_BATCH`
- `src/shared/batch-api-types.ts`: `BatchGetItemResult`, `BatchUnprocessedCode`
- `src/shared/types.ts`: `QueryItemsPage`
- `docs/agent-plans/2026-11-26-read-result-shape.md`

## Table of Contents

- [1. Overview and Context](#1-overview-and-context)
  - [1.1 The problem](#11-the-problem)
  - [1.2 Facts of the current system](#12-facts-of-the-current-system)
  - [1.3 Glossary](#13-glossary)
- [2. Goals and Requirements](#2-goals-and-requirements)
  - [2.1 In scope](#21-in-scope)
  - [2.2 Requirements](#22-requirements)
  - [2.3 Out of scope](#23-out-of-scope)
- [3. Milestones](#3-milestones)
- [4. Proposed Solution](#4-proposed-solution)
  - [4.1 High-level overview](#41-high-level-overview)
  - [4.2 Technical details](#42-technical-details)
    - [4.2.1 Public API](#421-public-api)
    - [4.2.2 Derived values](#422-derived-values)
    - [4.2.3 `forEach`](#423-foreach)
    - [4.2.4 `includeInResult`](#424-includeinresult)
    - [4.2.5 Skip rules and validation](#425-skip-rules-and-validation)
    - [4.2.6 Consistency](#426-consistency)
    - [4.2.7 Queries](#427-queries)
    - [4.2.8 `transactGet`](#428-transactget)
    - [4.2.9 GSIs](#429-gsis)
    - [4.2.10 The graph module](#4210-the-graph-module)
    - [4.2.11 Milestone 1: `apiGraphGetStep` and waves](#4211-milestone-1-apigraphgetstep-and-waves)
    - [4.2.12 Milestone 2: local continuation](#4212-milestone-2-local-continuation)
    - [4.2.13 Errors in milestones 1 and 2](#4213-errors-in-milestones-1-and-2)
    - [4.2.14 Milestone 3: an outcome for each operation, and resume](#4214-milestone-3-an-outcome-for-each-operation-and-resume)
    - [4.2.15 Limits](#4215-limits)
    - [4.2.16 Performance](#4216-performance)
    - [4.2.17 Invariants](#4217-invariants)
    - [4.2.18 Testing](#4218-testing)
- [5. Alternative Options](#5-alternative-options)
- [6. Frequently Asked Questions](#6-frequently-asked-questions)
- [Appendix A. Future designs](#appendix-a-future-designs)

## 1. Overview and Context

### 1.1 The problem

A common read pattern needs more than one round:

- Render a blog post from its URL slug. Read the slug, then the post. Then read its author, its last 20 comments,
  its related posts, and the latest posts of each of its tags.
- Show the details of an order. Read the order, then its lines. Then read the product of each line.
- Query a GSI for keys. Then read the base items of those keys.

Today the caller does each round itself. It calls `getItem`, extracts the keys, and then calls `batchGetItems` or
`queryItems` with them. Each round costs one round trip from the Worker to the partitions. Each application writes
the grouping, the dedup, and the failure handling again.

FokosDB needs one read-only operation that takes a graph of dependent reads. The operation must run the graph with
the fewest round trips and the fewest RPCs that the topology allows. Each partition must do as much of the graph as
it can before the graph makes another hop.

### 1.2 Facts of the current system

1. **All items of one hash key are in one partition**, until the routingHash of the hash key is promoted. After a
   promotion, a range tree holds the hash key. Each range partition holds one contiguous sort-key interval. Thus the
   strongest locality is "same hash key", and then "same sort-key interval".
2. **Different hash keys can share a leaf partition.** The topology cache of the client knows this.
   `FokosShardingClient.resolveAll` already groups keys by the partition that it knows.
3. **A local handler is synchronous.** All the reads of one handler call see one committed state of the partition.
   No other request runs between them.
4. **A partition knows exactly which keys it owns.** `FokosShardingRuntime.ownerCheck` answers synchronously. It
   reads the topology and the route overrides, and no cache.
5. **A partition serves only its own subtree.** A key outside it fails with `partition_misrouted`.
6. **The Worker can be far from the partitions.** A round trip from the Worker to a Durable Object (`L_w`) can be
   tens of milliseconds or more. A round trip between two Durable Objects near the data (`L_d`) is smaller.
   TODO: measure both. The number of `L_w` round trips is the main latency cost. The number of RPCs is the main
   billing cost.
7. **Only a partition evaluates expressions.** Expressions travel as trees, and a partition compiles them to SQL. The
   Worker has no expression evaluator.
8. **A read-only `group` operation can read through to its source during an import.** `apiBatchGetItems` uses
   `whileMigrating: "read_source"`.

### 1.3 Glossary

| Term | Meaning in this document |
| --- | --- |
| graph | The operations of one `graphGetItems` call. |
| operation | One named entry of `ops`: a `get`, a `query`, a `transactGet`, or a `forEach`. |
| read | One unit of work in a step: one get, one query, or one element of a `forEach`. |
| derived value | A value of an operation that the partition computes from the item of another operation (section 4.2.2). |
| element | One entry of the array that a `forEach` iterates over. |
| level | The depth of an operation in the dependency order of the graph. |
| step | One `apiGraphGetStep` RPC to one partition. |
| wave | In milestone 1, the set of steps that the Worker sends together and waits for together. |
| ready set | The reads whose derived values are all known. |
| frontier | The dependent reads with resolved keys that a partition returns because it does not own them. |
| local continuation | A partition runs a dependent read that it owns in the same handler call (milestone 2). |
| `L_w`, `L_d` | A round trip from the Worker to a Durable Object, and between two Durable Objects. |

## 2. Goals and Requirements

### 2.1 In scope

1. `FokosDB.graphGetItems` reads a graph of `get`, `query`, `transactGet`, and `forEach` operations, and returns one
   result for each operation.
2. An operation can use derived values from the item of another operation for its keys and its sort-key operands.
3. A `forEach` runs one `get` or one `query` for each element of an array, or for each item of an earlier result.
4. An operation can be skipped by a condition, and a `forEach` element can be skipped by its own condition.
5. An operation can read a GSI.
6. The partition that reads an item computes every derived value from it in SQL.
7. Milestone 1 sends at most one step to each partition in each wave.
8. From milestone 2, a partition runs each dependent read that it owns in the same handler call.
9. From milestone 3, a failed read fails only its own branch, and the caller can resume the graph.

### 2.2 Requirements

- The operation must be read-only. It takes no lock and needs no idempotency token.
- Each item read must be strongly consistent at the time of its read, as `getItem` is.
- The Worker must not evaluate an expression.
- A partition must continue only the reads of its own shard group.
- A partition must run only the reads that it owns.
- A query must be complete in its step. The graph returns no cursor.
- In milestones 1 and 2, the first failed read must fail the whole graph. The error must name the operation.
- A read that does not fit in the step answer must be `unprocessed`. It must not be dropped.
- Milestone 3 must not break the callers of milestones 1 and 2.
- One step answer must stay under the RPC limit.
- The graph module must do no I/O.

### 2.3 Out of scope

- **Writes.** A later write variant can run the graph, and then send `transactWriteItems` with conditions on the `v`
  of each item that it read.
- **Snapshot consistency across the whole graph.** It costs too much. Section 5 gives the reasons.
- **Pagination across a graph.** A query is complete in its step (section 4.2.7).
- **A callback synchronization point**, **a partition as a hub**, **values from two operations**, **values in a query
  filter**, **nested `forEach`**, **a snapshot of keys from an array**, and **batching optimizations.** Appendix A
  records each design.
- **A read coordinator Durable Object** and **answers sent directly to the Worker.** Section 5 gives the reasons.

## 3. Milestones

1. **The API and `apiGraphGetStep`, in waves (section 4.2.11).** The public types, the validation, and the acyclic
   check. The graph module in `shared/`. The planner and the executor in the Worker. `apiGraphGetStep` runs mixed
   gets, `forEach` elements, and queries in one RPC for each partition. The Worker runs each `transactGet` on the path
   of `transactGetItems`. The partition compiles the derived values, `{ ref: "element" }`, `skipIf`, and
   `skipElementIf` to SQL. The result has `stats`. A read that does not fit in the step answer is `unprocessed`
   (section 4.2.15). A failure fails the whole graph (section 4.2.13). This milestone delivers the full API with one
   `L_w` for each level.
2. **Local continuation (section 4.2.12).** The downstream graph in the step, `ownerCheck`, and the fixpoint. Eager
   scheduling in the Worker. `stats.localReads`. The `localContinuation` option and its property test. This milestone
   removes the `L_w` of each level inside one partition.
3. **Failure handling and resume (section 4.2.14).** `onFailure: "collect"`, the outcomes and their propagation,
   `complete` and `failures`, `retry` and `given`, and `FokosStd.graphGetItemsWithRetry`. This milestone keeps the
   results of the branches that did not fail.
4. **Limits (section 4.2.15).** Uniform limits for all steps, from the accumulated item count and the accumulated
   size of the graph.

## 4. Proposed Solution

### 4.1 High-level overview

The caller gives a map of named operations. Each operation is a member of a discriminated union on `op`. A key or a
sort-key operand can be a literal, or a derived value: `{ from: "<operation>", path | key | expr }`.

The graph is data, so it travels over RPC. The Worker plans it:

1. The Worker validates the graph and finds the dependencies.
2. The Worker finds the ready set, and groups the reads by partition with `resolveAll`.
3. The Worker sends one step RPC to each partition. Each step carries the reads, and the expressions of their direct
   dependents.
4. The partition runs the reads in one synchronous handler call. In the same SQL statement, it computes the derived
   values of the dependents.
5. The partition returns the results and the derived values. The Worker binds the values as literals, and goes to
   step 2.

```
 Worker                     partition A (slug#hello-world)   partition B (post#42)        partition C (user#ada)
 ──────                     ──────────────────────────────   ─────────────────────        ──────────────────────
 validate, plan
 wave 1: step ────────────▶ get slug
                            compute: the post key
         ◀──────────────── derived values
 wave 2: step ─────────────────────────────────────────────▶ get post
                                                             compute: authorId, the keys
                                                             of the related posts, the tags
         ◀───────────────────────────────────────────────── results + derived values
 wave 3: step ─────────────────────────────────────────────▶ query comments
                                                             (same hash key as the post)
         step ──────────────────────────────────────────────────────────────────────────▶ get author
         ◀───────────────────────────────────────────────── results
         ◀────────────────────────────────────────────────────────────────────────────── results
 results, stats
```

From milestone 2, the partition does more. When it owns the key of a dependent read, it runs that read in the same
handler call, and repeats until no owned read is ready. It returns only the frontier. In the diagram, partition B
queries the comments in wave 2, in the same handler call as the post, so the graph needs one `L_w` less.

The partition computes each derived value. Thus the Worker never evaluates an expression, and an item used only to
find other keys does not cross the network. Each part of the graph that one partition completes in one handler call
is one consistent snapshot.

In milestones 1 and 2, the first failed read fails the whole graph. The error names the operation. A read that does
not fit in the step answer is `unprocessed`. Milestone 3 adds an outcome for each failed operation and a resume.

### 4.2 Technical details

#### 4.2.1 Public API

```ts
const data = (path: string) => ({ ref: "data", path }) as const;
const element = (path?: string) => ({ ref: "element", path }) as const;
const lit = (val: string) => ({ val }) as const;
const concat = (...args: ExpressionValue[]) => ({ fn: "sqlite.concat", args }) as const;

const { results, stats } = await db.graphGetItems({
	ops: {
		// The URL gives the slug. The slug item gives the key of the post, for example "post#42".
		slug: {
			op: "get",
			hashKey: "slug#hello-world",
			onNotFound: "fail", // the page is a 404 without it
			includeInResult: false, // the page needs the post, not the slug item
		},
		post: { op: "get", hashKey: { from: "slug", path: "$.postKey" }, sortKey: "meta" },
		// The same hash key as the post, so the same partition.
		comments: {
			op: "query",
			hashKey: { from: "post", key: "hashKey" },
			sortKeyCondition: { op: "begins_with", prefix: "comment#" },
			scanIndexForward: false,
			limit: 20,
		},
		// The author is under another hash key, so the graph makes one hop.
		author: { op: "get", hashKey: { from: "post", path: "$.authorId" }, sortKey: "profile" },
		// $.related is [{ "id": "17" }, { "id": "33", "draft": true }, ...]: one get for each related post.
		related: {
			op: "forEach",
			from: "post",
			path: "$.related",
			skipElementIf: { op: "eq", args: [element("$.draft"), { val: true }] },
			do: {
				op: "get",
				hashKey: { expr: concat(lit("post#"), element("$.id")) },
				sortKey: "meta",
				projection: [{ expr: data("$.title"), as: "title" }],
			},
		},
		// $.tags is ["cloudflare", "databases"]: the latest 3 posts of each tag, a query for each element.
		moreFromTags: {
			op: "forEach",
			from: "post",
			path: "$.tags",
			do: { op: "query", hashKey: { expr: concat(lit("tag#"), element()) }, scanIndexForward: false, limit: 3 },
		},
		// The next part of the series of the post: a derived sort-key operand, and a conditional read.
		nextInSeries: {
			op: "query",
			hashKey: { from: "post", path: "$.seriesId" },
			sortKeyCondition: { op: "gt", value: { from: "post", expr: concat(lit("part#"), data("$.seriesPart")) } },
			limit: 1,
			skipIf: { from: "post", condition: { op: "not_exists", args: [data("$.seriesId")] } },
		},
	},
});

results.slug; //         { outcome: "ok", count: 1 }  (no item: includeInResult is false)
results.post; //         { outcome: "ok", count: 1, partitionIndex: 1, item: { ... } }
results.comments; //     { outcome: "ok", count: 20, scannedCount: 20, items: [...] }
results.author; //       { outcome: "ok", count: 1, partitionIndex: 2, item: { ... } }
results.related; //      { outcome: "ok", count: 3, elements: [GraphGetResult, ...] }, element i of $.related
results.moreFromTags; // { outcome: "ok", count: 6, elements: [GraphQueryResult, ...] }, element i of $.tags
results.nextInSeries; // { outcome: "skipped", reason: "skip_if" } when the post is in no series
stats; //                { waves: 3, stepRpcs: 7, itemsRead: 34, bytesRead: 41250 }
```

The operation types:

```ts
/** A value from the item of another operation. The partition that reads `from` computes it in SQLite. */
type FromValue =
	| { from: string; key: "hashKey" | "sortKey" }
	| { from: string; path: string }
	| { from: string; expr: ExpressionValue };

/**
 * A value inside `forEach.do`: an expression over the current element (`{ ref: "element", path? }`) and the
 * item of `forEach.from` (`data`, `hashKey`, `sortKey`, `v`, `ttlAt`).
 */
type ElementValue = { expr: ExpressionValue };

/** `SortKeyCondition`, with each sort-key operand also as a value of kind `V`. */
type GraphSortKeyCondition<V> =
	| { op: "eq" | "lt" | "lte" | "gt" | "gte"; value: SortKey | V }
	| { op: "between"; lower: SortKey | V; upper: SortKey | V }
	| { op: "begins_with"; prefix: SortKey | V }
	| { op: "range"; lower?: { value: SortKey | V; inclusive: boolean }; upper?: { value: SortKey | V; inclusive: boolean } };

type GetOp<V> = {
	op: "get";
	hashKey: HashKey | V;
	sortKey?: SortKey | V;
	index?: string; // a GSI. Without it, the base table.
	projection?: readonly ProjectionExpression[];
	onNotFound?: "skip" | "fail"; // the default is "skip": the dependents are skipped with "from_not_found"
};

type QueryOp<V> = {
	op: "query";
	hashKey: HashKey | V;
	sortKeyCondition?: GraphSortKeyCondition<V>;
	scanIndexForward?: boolean;
	index?: string;
	filter?: ConditionExpression;
	projection?: readonly ProjectionExpression[];
	limit?: number; // the most items that the caller wants
};

type TransactGetOp = {
	op: "transactGet";
	items: readonly { hashKey: HashKey | FromValue; sortKey?: SortKey | FromValue; projection?: readonly ProjectionExpression[] }[];
};

type ForEachOp = {
	op: "forEach";
	from: string;
	path?: string; // the array in the item of `from`. Absent when `from` is a query or a forEach (section 4.2.3).
	skipElementIf?: ConditionExpression; // over the element and the item of `from`
	do: GetOp<ElementValue> | QueryOp<ElementValue>;
};

/** The properties of every operation. */
type GraphOpCommon = {
	skipIf?: { from: string; condition: ConditionExpression };
	includeInResult?: boolean; // the default is true (section 4.2.4)
	onMissing?: "skip" | "fail"; // a `path` that is absent. The default is "skip".
};

type GraphOp = (GetOp<FromValue> | QueryOp<FromValue> | TransactGetOp | ForEachOp) & GraphOpCommon;

type GraphGetItemsOptions = {
	ops: Record<string, GraphOp>;
};
```

The result types:

```ts
/** Every operation that ran. `count` is the number of items that the operation found. */
type GraphOk = { outcome: "ok"; count: number };

// `item` and `items` have the shapes of the other read methods: `ReadResultItem` is the full `ReadItem`, or
// `{ kind: "projected", data }` with a projection. An absent `item` means "not found".
type GraphGetOk<T> = GraphOk & { partitionIndex: number; item?: ReadResultItem<T> }; // count is 0 or 1
type GraphQueryOk<T> = GraphOk & { items: ReadResultItem<T>[]; scannedCount: number }; // as QueryItemsPage, no cursor
type GraphTransactGetOk<T> = GraphOk & { items: TransactGetItemResult<T>[] };
type GraphForEachOk<R> = GraphOk & { elements: R[] }; // R is the result of `do`. count is the sum of their counts.

/** An operation with `includeInResult: false`. It ran, and only its count comes back. */
type GraphOmittedOk = GraphOk & { item?: never; items?: never; elements?: never };

type GraphNotRunFields = { item?: never; items?: never; elements?: never };
/** The graph decided not to run the operation. It is not a failure. */
type GraphSkipped = GraphNotRunFields & {
	outcome: "skipped";
	reason: "skip_if" | "from_not_found" | "from_skipped" | "missing_path" | "null_key";
};
/** The operation did not run, or did not fit in the step answer (section 4.2.15). Milestone 3 adds more codes. */
type GraphUnprocessed = GraphNotRunFields & {
	outcome: "unprocessed";
	code: BatchUnprocessedCode | "dependency_unprocessed";
	error_id?: string;
};
/** Milestone 3 (section 4.2.14). */
type GraphRejected = GraphNotRunFields & { outcome: "rejected"; code: FokosErrorCode; error_id: string };
type GraphNotRun = GraphSkipped | GraphUnprocessed | GraphRejected;

type GraphGetResult<T> = GraphGetOk<T> | GraphNotRun;
type GraphQueryResult<T> = GraphQueryOk<T> | GraphNotRun;

/**
 * The result of one operation. When the type of `Op` shows `includeInResult: false`, the result is
 * `GraphOmittedOk`. When the type does not show it, the result is the union of both.
 * `OkOf` selects the `Ok` type from `Op["op"]`.
 */
type ResultOf<Op> = (Op extends { includeInResult: false } ? GraphOmittedOk : OkOf<Op>) | GraphNotRun;

type GraphGetItemsResult<Ops> = {
	results: { [K in keyof Ops]: ResultOf<Ops[K]> };
	stats: {
		waves: number; // milestone 1. From milestone 2, the longest chain of steps.
		stepRpcs: number;
		itemsRead: number;
		bytesRead: number;
		localReads?: number; // milestone 2: the reads that a partition ran without a return to the Worker
	};
	meta: FokosMeta;
};

// db.graphGetItems<Ops extends Record<string, GraphOp>>(opts: GraphGetItemsOptions & { ops: Ops }): Promise<GraphGetItemsResult<Ops>>
```

The result follows the rules of the other read methods (`docs/agent-plans/2026-11-26-read-result-shape.md`):

- `outcome` says whether the operation ran. `"ok"` is the same value as in `BatchGetItemResult`. `if (r.outcome ===
  "ok")` and `r.count` work for every kind of operation.
- An absent `item` means "not found". No result has `found`, and no result echoes the keys of the request.
- `partitionIndex` points into `meta.partitions`, as in `batchGetItems` and `transactGetItems`. A query can visit
  more than one partition, so its result has no `partitionIndex`, as a query page has none.
- `count` and `scannedCount` have the meaning of `QueryItemsPage`.
- `count` always says how many items an operation found, also with `includeInResult: false`. Thus the result is
  never ambiguous, and it never holds a fake item.
- The types force a check: `results.related.elements` exists only after a check of `outcome === "ok"`.

#### 4.2.2 Derived values

A value of an operation is a literal, or a derived value:

- `{ from, key: "hashKey" | "sortKey" }`: a key of the item of `from`.
- `{ from, path }`: one JSON path of the item of `from`.
- `{ from, expr }`: one expression tree over the item of `from`, of the same kind as a projection. For example,
  `{ fn: "sqlite.concat", args: [{ val: "author#" }, data("$.authorId")] }`.

A derived value can be a key or a sort-key operand. The dependencies of the graph are the `from` of its derived
values, of `skipIf`, and of `forEach`.

The partition that reads the item of `from` computes the derived value. It computes it as an extra projection column
of the same statement, and returns it. The Worker binds the value as a literal in the step of the dependent.

This rule gives three properties:

- The Worker never evaluates an expression (fact 7 of section 1.2).
- No item data crosses the network only to compute a value.
- From milestone 2, the partition knows the next keys at once, and it can continue the graph.

One derived value has one `from`. Appendix A.2 gives the design for a value from two operations.

#### 4.2.3 `forEach`

A `forEach` runs its `do` operation one time for each element. The partition that reads the item of `from` computes
the values of `do` one time for each element.

**The elements:**

- With `path`, the elements are the array at `path` in the item of `from`.
- When `from` is a `query`, or a `forEach` of `get`, `path` is absent. The elements are the items of that result, in
  their order. Then `{ ref: "element", path }` reads the data of the current item.

**The values of `do`** are `{ expr }` trees. They can use one new reference, `{ ref: "element", path? }`: the current
element, or one JSON path of it. The other references (`data`, `hashKey`, `sortKey`, `v`, `ttlAt`) refer to the item
of `from`. When the elements are the keys themselves, the value is `{ expr: { ref: "element" } }`.

**The order of evaluation for each element:**

1. When `skipElementIf` is present, the partition evaluates it. When it is true, the element is `skipped` with the
   reason `skip_if`, and the values do not run.
2. The partition computes the values. When a key is NULL, the element is `skipped` with the reason `null_key`. An
   absent path gives NULL in SQLite, so an element without `$.id` gives `null_key`.
3. When a key is not text or bytes, or is above the key size limit, the read fails with `invalid_key`. The error
   names the element.
4. Otherwise, `do` runs.

**The other rules:**

- The result is positional. `elements[i]` is the result of element `i`, also when the element was skipped. The caller
  can match the results to its array. The reason on each skip shows a wrong path: every element shows `null_key`.
- Each get of a `forEach` is strongly consistent on its own, as in `batchGetItems`. From milestone 2, when the
  elements are in one partition, that partition reads them in one handler call. Then they are one snapshot.
- When `path` is absent, the operation is `skipped` with `missing_path`. With `onMissing: "fail"`, it fails.
- When the value at `path` is not an array, the operation fails with `for_each_not_array`.
- `do` must not be a `forEach`. A `forEach` can have another `forEach` as its `from`.

The partition computes a `forEach` with one statement over `json_each` of the array. The statement runs in the same
handler call as the read of the item of `from`:

```sql
SELECT e.key AS position, <skipElementIf> AS skip, <hashKey value> AS hk, <sortKey value> AS sk
FROM items i, json_each(i.data, ?path) e
WHERE i.routing_hash = ? AND i.hk = ? AND i.sk = ?
ORDER BY e.key
```

The compiler gives `{ ref: "element", path }` as `json_extract(e.value, ?path)`, and `{ ref: "element" }` as
`e.value`. The validation accepts `{ ref: "element" }` only in `forEach.do` and in `skipElementIf`. Anywhere else, it
fails before any I/O.

#### 4.2.4 `includeInResult`

With `includeInResult: false`, the graph reads the items of the operation and computes values from them, but the
result does not hold the items. The result is `GraphOmittedOk`: `{ outcome: "ok", count }`. `count` is the number of
items that the operation found. The option applies to every kind of operation, also to an operation that reads many
items. The use is an operation for navigation only. For example, the page needs the post, and not the slug item that
gives the key of the post.

The option also removes bytes from the network. The partition that reads the slug item computes the key of the post,
and returns only that key. From milestone 2, when the dependents are in the same partition, the item does not leave
the partition. An item that only gives keys costs only those keys on the network.

#### 4.2.5 Skip rules and validation

- `skipIf` is a condition expression over the item of `from`. When it is true, the operation is `skipped` with
  `skip_if`.
- When the item of `from` is absent, the dependent is `skipped` with `from_not_found`. When `from` was skipped, the
  dependent is `skipped` with `from_skipped`.
- When a `path` of a derived value is absent, the operation is `skipped` with `missing_path`. With
  `onMissing: "fail"`, the operation fails with `read_graph_missing_path`.
- With `onNotFound: "fail"` on a `get`, an absent item fails the graph with `read_graph_item_not_found`.
- `skipped` is a decision of the graph, not a failure. The graph continues.
- `index` names a GSI (section 4.2.9). A `transactGet` reads only the base table.

The client checks these rules before any I/O:

- The graph is acyclic.
- Each `from` names an operation of the graph.
- The `from` of a `FromValue` or of a `skipIf` is a `get`, because the value needs one item.
- The `from` of a `forEach` is a `get` (with `path`), a `query`, or a `forEach` of `get` (without `path`).
- Any other `from` fails the validation.

#### 4.2.6 Consistency

- **One item.** Each item read is strongly consistent at the time of its read, as `getItem` is.
- **One handler call.** All items that one partition reads in one handler call are from one committed state (fact 3
  of section 1.2). Thus each part of the graph that one partition completes in one handler call is one consistent
  snapshot. From milestone 2, when one partition completes the whole graph in one handler call, the whole result is
  one snapshot, at no extra cost.
- **Different steps.** Items that different steps read can be from different committed states. When two operations
  read one item in two different steps, the two results can show two different versions. The executor does not
  reuse an item from an earlier step.
- **`transactGet`** gives one snapshot over the items of that one operation, as `transactGetItems` does. It gives no
  isolation with the other operations of the graph.
- **Retries.** The method does not retry. The graph is read-only, so the caller can always send it again.

#### 4.2.7 Queries

A graph has no cursors. A query reads until `limit` or the end of its sort-key range. Then the query is complete, and
its dependents run.

- The partition reads a query in the same synchronous handler call as the other reads of the step. Every operation
  of a step is local and synchronous in its partition.
- A query routes by its hash key. When the hash key is promoted, the step item reaches the partition that holds the
  route override (the partition or an ancestor). That partition first finishes all its local reads of the step in
  its synchronous handler call. Then it dispatches the query as an ordinary `apiQueryItems` range request, which walks
  the range tree for the sort-key interval. It adds the result to its part of the step answer. No partition reads
  keys that it does not own.
- When the items of a query do not fit in the step answer, the query is `unprocessed` with `response_too_large`
  (section 4.2.15). A query never returns a part of its items.

#### 4.2.8 `transactGet`

A `transactGet` operation is the same as a `transactGetItems` call. When all its derived values are known, the
Worker runs it on the path of `transactGetItems`: `#readSnapshotFastPath`, or the two-phase read of
`#readTransaction`. It runs in parallel with the steps of the graph.

- It is never part of an `apiGraphGetStep` step, and a partition never continues into it.
- It has the errors of `transactGetItems`: for example `read_conflict`, `pending_write`, and `partition_migrating`
  during an import.

The order details page shows the use. The payment and its refunds must agree, so they are one snapshot:

```ts
await db.graphGetItems({
	ops: {
		order: { op: "get", hashKey: "user#7", sortKey: "order#123", onNotFound: "fail" },
		// The same hash key as the order, so the same partition.
		lines: {
			op: "query",
			hashKey: { from: "order", key: "hashKey" },
			sortKeyCondition: { op: "begins_with", prefix: "order#123#line#" },
		},
		// A forEach from a query: one get for each line, in other partitions.
		products: {
			op: "forEach",
			from: "lines",
			do: { op: "get", hashKey: { expr: concat(lit("product#"), element("$.productId")) }, sortKey: "meta" },
		},
		payment: {
			op: "transactGet",
			items: [
				{ hashKey: { from: "order", path: "$.paymentId" }, sortKey: "payment" },
				{ hashKey: { from: "order", path: "$.paymentId" }, sortKey: "refunds" },
			],
		},
	},
});
```

#### 4.2.9 GSIs

An operation with `index: "byAuthor"` reads the GSI `byAuthor`. A GSI is a shard group of its own, and the
`FokosDBPartitionDO` class serves its partitions. Thus a GSI partition runs `apiGraphGetStep`. From milestone 2, it
continues the graph as a base partition does.

```ts
await db.graphGetItems({
	ops: {
		posts: {
			op: "query",
			index: "byAuthor",
			hashKey: "author#ada",
			sortKeyCondition: { op: "begins_with", prefix: "2026-" },
			limit: 10,
		},
		// The GSI projection of byAuthor keeps $.postKey, so the GSI item has the key of the base item.
		bodies: { op: "forEach", from: "posts", do: { op: "get", hashKey: { expr: element("$.postKey") } } },
	},
});
```

- **Base keys are attributes.** The graph has no special reference to `baseHashKey` and `baseSortKey`. When a caller
  needs the base item, the projection of the GSI must keep the key of the base item as an attribute.
- **A partition continues only the reads of its own shard group.** A GSI partition owns no key of the base table. A
  base partition owns no key of a GSI. Such a read always goes to the frontier.
- **A GSI is eventually consistent.** A GSI item can name a base item that a later write deleted. Then the base read
  gives `not_found`. The base item can also have changed, so that it no longer matches the query. The graph does not
  check the query of the GSI again on the base item. A caller that needs this check adds a `skipIf` on the base read.

#### 4.2.10 The graph module

The graph logic is one pure, synchronous module in `shared/`. It does the validation, the dependencies, the ready
set, and the binding of derived values. It does no I/O.

- The Worker uses the module with `resolveAll`.
- From milestone 2, the partition uses the module with `ownerCheck`.

#### 4.2.11 Milestone 1: `apiGraphGetStep` and waves

`apiGraphGetStep` is a new `group` operation in `PartitionOps`:

| Field | Value |
| --- | --- |
| `shape` | `"group"` |
| `readOnly` | `true` |
| `whileMigrating` | `"read_source"` |
| `admissionTag` | `"read"` |
| `failurePolicy` | `"fail_fast"` |

The items of a step are its reads. A query routes by its hash key. When the hash key is promoted, the partition that
holds the route override dispatches the query as an `apiQueryItems` range request after its local reads (section
4.2.7).

The Worker runs the graph in waves:

1. The Worker finds the ready set. The first wave holds each operation with literal values, also when the operation
   is on a later level.
2. The Worker groups all the ready reads by the partition that `resolveAll` gives. Each partition gets one step with
   all its ready reads of the wave: gets, `forEach` elements, and queries. Each ready `transactGet` runs on its own
   path in parallel (section 4.2.8).
3. Each step carries the derived values and the `skipIf` conditions of the direct dependents of its reads. The
   partition computes them in SQL and returns them with its results. The partition does not run a dependent, also
   when it owns the key.
4. The Worker waits for all the steps of the wave. It binds the returned values, and goes to 1.

A wave sends at most one step to each partition. A dependent that the same partition owns waits for the next wave.

The step answer holds the results and the derived values, up to the byte limit of section 4.2.15. Each part of a
step stays inside the limit, but the parts together can go above it when a router joins them. The join marks each
read that does not fit as `unprocessed` with `response_too_large`, as the `merge` of `apiBatchGetItems` does.

#### 4.2.12 Milestone 2: local continuation

The step request also carries the downstream part of the graph, and the derived values that other partitions already
returned for it. The partition does these steps in one synchronous handler call:

1. The partition runs each read. In the same statement, it computes the derived values of the dependents.
2. For each derived key of its own shard group, the partition calls `ownerCheck`. A key of another shard group is
   never local. The partition runs each dependent read that it owns. It repeats until no owned read is ready (a
   fixpoint), or until the step answer is full (section 4.2.15).
3. The partition answers with the results and the frontier.

The handler is synchronous, so the rule of `ownerCheck` holds: no checker is kept across an `await`.

**During an import,** the handler needs no check of its own. With `whileMigrating: "read_source"`, the runtime runs
the step on the source, and the handler never runs on the partition that imports. On a source that split,
`ownerCheck` returns false for every key, so every dependent read goes to the frontier. On a promotion source,
`ownerCheck` is exact, so the source continues only the keys that it still owns.

**Scheduling in the Worker.** The Worker does not use waves. When a step answer arrives, the Worker sends its
frontier at once, grouped by partition. It coalesces for one microtask, so that answers that arrive together share
RPCs. A slow partition then delays only its own dependents. A partition can get more than one step at the same time.

**The option `localContinuation: false`** is internal. It turns the continuation off. It is a kill switch, and the
property test of section 4.2.18 uses it.

#### 4.2.13 Errors in milestones 1 and 2

The first read that fails makes the whole graph fail. The method throws, and returns no partial result.

- **The error is the error of the read.** It keeps the class and the code that the partition or the client raised.
  Examples: `partition_migrating`, `read_conflict`, `read_graph_item_not_found`, a projection error. The caller
  handles the same codes as for the other methods.
- **The error names the operation.** It gets the attribute `graphGet: { op, index? }`. `op` is the name of the
  operation. `index` is the position of the element in a `forEach`.
- **The error carries `meta`** with the partitions that answered so far, as the errors of `transactGetItems` do
  (`withMetaSoFar`).
- **One error.** When more than one read fails, the Worker throws the first failure that it sees. Then it sends no
  new step, and it ignores the answers of the steps in flight.
- **No retry of a step.** The Worker sends each step one time. A transport error fails the graph.
- **`failurePolicy: "fail_fast"`** is a policy that the runtime already has. The step answer has no failure parts.
- **A retry is the whole graph again.** It is always safe, because the graph is read-only. The caller decides from
  the code if a retry can succeed.

The result types of milestones 1 and 2 hold only these outcomes: `ok`, `skipped`, and `unprocessed` with
`response_too_large` or `dependency_unprocessed` (section 4.2.15).

#### 4.2.14 Milestone 3: an outcome for each operation, and resume

Milestone 3 adds the option `onFailure: "collect"`. The default stays `"throw"` (section 4.2.13), so milestone 3 does
not break the callers of milestones 1 and 2.

With `"collect"`, a failed read fails only its own branch. The method does not throw after I/O starts. Each
operation has one outcome:

| Outcome | Meaning | Can a retry succeed? |
| --- | --- | --- |
| `ok`, with `count` | The operation ran. | — |
| `skipped`, with a reason | The graph decided not to run it. It is a success. | — |
| `unprocessed`, with a `code` | It did not run, or a transient error stopped it: a transport error, `partition_migrating`, `read_conflict`, `response_too_large`, `dependency_unprocessed`. | Yes |
| `rejected`, with `code`, `error_id`, `attributes` | The same request fails the same way again: a projection that is not valid, `dependency_rejected`. | No |

The result gets three fields:

```ts
complete: boolean; // no operation, and no element of a forEach, is unprocessed or rejected
failures: { op: string; index?: number; outcome: "unprocessed" | "rejected"; code: string }[];
retry?: GraphGetItemsOptions; // the same ops, with `given` set to the results that completed
```

The rules:

- **A dependent takes the class of the failure of its parent.** A dependent of an `unprocessed` operation is
  `unprocessed` with `dependency_unprocessed`. A dependent of a `rejected` operation is `rejected` with
  `dependency_rejected`. Thus `skipped` always means a decision of the graph, and `complete` is one check.
- **A partial input never feeds a dependent.** When one element of a `forEach` is `unprocessed` or `rejected`, an
  operation with that `forEach` as its `from` fails in the same class.
- **`given`.** `given: { slug: <a result> }` gives the result of an operation that the caller already has. The
  graph does not read it again. `given` also lets a caller inject a cached item, for example a cached slug. A
  result in `given` can be older than the items that the retry reads (section 4.2.6). The operations in `given` join
  the ready set at the start.
- **`retry`** is the same `ops`, with `given` set to each result that completed. A retry reads only what is missing.
- **`FokosStd.graphGetItemsWithRetry`** sends `retry` again with backoff. It stops when the graph is complete, when
  only `rejected` failures stay, or when the attempts run out.
- **`apiGraphGetStep` changes to `failurePolicy: "collect"`**, with failure parts, as `apiBatchGetItems` has.

#### 4.2.15 Limits

Milestones 1 to 3 have one limit: the bytes of one step answer, `MAX_PAYLOAD_BYTES_PER_BATCH` (24 MiB).

- A partition adds each read to its answer while the answer stays inside the limit.
- A read that does not fit is `unprocessed` with `response_too_large`. Its dependents are `unprocessed` with
  `dependency_unprocessed`.
- The caller sends the graph again, or a smaller graph, for the `unprocessed` reads. From milestone 3, `retry` and
  `given` do this.

Milestone 4 adds uniform limits for all steps, from the accumulated item count and the accumulated size of the graph.
TODO: the values.

#### 4.2.16 Performance

`D` is the number of levels that cross a partition boundary. The figures use `L_w = 80 ms` and `L_d = 5 ms` as
examples. TODO: measure both.

| Design | Latency | `D = 1` | `D = 3` |
| --- | --- | --- | --- |
| Milestone 1, waves | `(levels) × L_w` | 160 ms or more | 320 ms or more |
| Milestone 2, local continuation | `(D + 1) × L_w` | 160 ms | 320 ms |
| Milestone 2, all in one partition | `1 × L_w` | 80 ms | 80 ms |
| A partition as a hub (Appendix A.1), best case | `L_w + D × L_d` | 85 ms | 95 ms |

- **The main gain of milestone 2:** a chain inside one hash key completes in one `L_w`, at any depth. Examples are the
  order and its lines, or the post and its comments. The data model gets this when it puts related items under one
  hash key.
- **Other local reads:** hash keys that share a leaf continue locally, because `ownerCheck` is exact. A promoted hash
  key continues locally while the sort keys stay in the interval of the range partition.
- **RPC count:** each wave sends at most one step to each partition.
- **Bytes:** a derived value replaces the item that it comes from on the network (section 4.2.4).
- **Learned routes:** each step answer carries the routing metadata of today. The topology cache then improves the
  grouping of the next step.

#### 4.2.17 Invariants

| Invariant | Mechanism |
| --- | --- |
| The Worker evaluates no expression. | The partition of `from` computes each derived value (section 4.2.2). |
| A partition continues only the reads that it owns. | `ownerCheck`, for the keys of its own shard group only. |
| A partition continues during an import only with keys that it owns. | `read_source` runs the step on the source, and `ownerCheck` decides there. |
| A wave sends at most one step to each partition. | The Worker groups the ready set of the wave with `resolveAll`. |
| One step answer stays under the RPC limit. | A read that does not fit is `unprocessed` (section 4.2.15). |
| A query in a result is complete. | A query that does not fit is `unprocessed` as a whole (section 4.2.7). |
| The graph is read-only. | `apiGraphGetStep` is `readOnly: true` with the `read` admission tag. |

#### 4.2.18 Testing

- A property test compares the results with `localContinuation: false` and with the continuation on. The results
  must be the same.
- Each milestone writes its own tests with its implementation. The tests cover the graph during a split, a
  promotion, and an import.

## 5. Alternative Options

- **Snapshot consistency across the whole graph** (`consistency: "snapshot"`). It costs too much, so the graph does
  not offer it. Each part of the graph that one partition completes in one handler call is already one snapshot. The
  design uses the method of `#readTransaction`: two reads, then a compare.
  1. Phase 1 is the graph itself. Each step reports `hasPendingWrite`, `v`, and `maxDeletedV`, as
     `txReadForTransaction` does. A pending write aborts the graph with `pending_write`.
  2. Phase 2 is one parallel round to each partition that phase 1 touched. It reads each item again with a key-only
     projection, and runs each query again with a key-only projection.
  3. The Worker compares each item as `#readTransaction` does. Each query compares its list of keys and versions.
  4. A difference aborts the graph with `read_conflict`.

  The costs: one more `L_w` round for each graph, a second read of each query range, and more aborts when the graph
  is large and its steps take a long time. A retry cannot use `given`, because a snapshot needs both phases over every
  item. When one partition completed the whole graph in one handler call, phase 2 is not necessary.

- **A callback API with automatic batching** (`db.readFlow(async (r) => { ... })`, a DataLoader). The executor groups
  the reads that the callback starts in one microtask. A closure cannot travel over RPC. Thus each level costs one
  `L_w`, and no partition can continue the graph. The callback synchronization point (Appendix A.3) gives the same
  flexibility only where the caller needs it.
- **A `batchGet` operation whose keys are one expression that returns an array.** The expression engine has no
  function that builds an array. A `forEach` with `do: { op: "get" }` gives the same reads.
- **String references,** for example `$steps.post.item.authorId`. They need a parser, and they overload `ref`.
  `{ from, ... }` and the typed references of the expression engine replace them.
- **A read coordinator Durable Object** that drives the whole graph near the data. The Worker drives the graph, and
  a partition can drive a part of it as a hub (Appendix A.1).
- **Answers sent directly to the Worker** through an RPC stub in the request, while only control data goes along a
  chain of hubs. It is not necessary without hubs.

## 6. Frequently Asked Questions

### Why is `graphGetItems` a separate read-only operation, and not one operation for reads and writes?

A read-only operation has no locks, no idempotency token, and no unknown outcome. A retry of any part is always safe.
It can use `read_source` during a migration, and the `read` admission tag. It can run on any node, in any order, any
number of times. This lets a partition continue the graph without a correctness argument for each hop. The split
follows `transactGetItems` and `transactWriteItems`.

### Why does the partition compute the derived values, and not the Worker?

Only a partition evaluates expressions (fact 7 of section 1.2). The partition computes the values in the statement
that reads the item, so no extra read is necessary. The item does not cross the network to give a key. The partition
also knows the next keys at once, which local continuation needs.

### Why does an item read in two steps give two versions?

The executor does not reuse an item from an earlier step. Each read is strongly consistent at the time of its read.
Two steps run at different times, so a write between them is visible to the second step.

### Why does milestone 1 fail the whole graph on one failure?

It keeps milestone 1 small. The step uses `fail_fast`, which the runtime already has. Milestone 3 adds the outcomes
and the resume, and keeps `"throw"` as the default.

## Appendix A. Future designs

### A.1 A partition as the hub of a part of the graph

A partition with a frontier can drive a part of the remaining graph itself, in place of a return to the Worker. It
sends the frontier to the owners with an internal `FokosShardingClient`, as the coordinator and the materialization
Durable Object do. It waits for the answers, and continues locally with them. The partition decides this locally.

Good cases:

- **The graph comes back.** The dependents of the frontier have keys that this partition owns. For example, the post
  in partition A names a list in partition B, and the list names comments under the hash key of the post. Without a
  hub, the path is A, Worker, B, Worker, A: three `L_w`. With A as the hub, the cost is one `L_w` and one `L_d`.
- **A small frontier with many levels after it.** One hop to one partition, followed by more levels.

The partition can find these cases in the graph. A derived value that refers only to keys of its own operations
(`{ from, key: "hashKey" }`) resolves to it. A literal key that it owns resolves to it.

The limits:

- The step request carries a hop budget and a deadline, so that the chain of hubs has a bound.
- A hot partition, for example a popular post, must not aggregate every graph that starts there. The partition
  can decline to be a hub when it is busy. Then it returns the frontier to the Worker.
- The answer bytes cross one more hop through the hub.
- A callback operation (Appendix A.3) stops the hub. Only the Worker can run it.

### A.2 Other extensions

- **A value from two operations,** for example `concat(a.x, b.y)`. The Worker binds the value that one parent returned
  as a literal. The partition of the other parent computes the expression.
- **Derived values in a query `filter`.** A leaf `{ from, path | key | expr }` in the condition tree. The Worker
  replaces it with `{ val }` before the step.
- **Nested `forEach`.**
- **A snapshot of keys from an array**, when the elements are in more than one partition. It is a separate
  operation, for example `{ op: "transactGetEach", from, path, hashKey, sortKey }`. `transactGet` keeps a fixed list
  of keys.
- **Batching in the Worker.** The Worker can hold a ready read for partition A when an answer from partition B can
  give more reads for A. Then A gets one step, not two.
- **Dedup of keys inside a step.** TODO: a follow-up.
- **Topology in the partition.** A partition can get the topology from the topology cache, as the Worker does, and
  route a frontier read deeper. It is an optimization: the forwarding of today is already correct.
- **Fused reads in one partition.** The gets of one hash key can be one `WHERE (routing_hash, hk, sk) IN (...)`
  statement.

### A.3 A callback synchronization point

An operation `{ op: "callback", after: [...], run: (results) => GraphGetItemsOptions }` runs in the caller Worker when
its inputs are known. It gets the results so far, and returns more operations, which join the graph. It is for a
decision that expressions cannot make. The callback costs one `L_w`. The operations that do not depend on it continue
at the same time. A partition cannot continue the graph past a callback, and cannot act as a hub past it.
