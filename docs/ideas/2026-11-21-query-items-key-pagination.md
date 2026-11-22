# RFC — `queryItems` pagination with keys: `exclusiveStartKey` and `lastEvaluatedKey`

**State:** Draft
**Date:** 2026-11-21
**Author:** Lambros

## Table of contents

- [1. Overview and context](#1-overview-and-context)
- [2. Goals and requirements](#2-goals-and-requirements)
- [3. Milestones](#3-milestones)
- [4. Proposed solution](#4-proposed-solution)
- [5. Alternative options](#5-alternative-options)
- [6. Frequently asked questions](#6-frequently-asked-questions)
- [7. References](#7-references)

## 1. Overview and context

### 1.1 The problem

`queryItems` and `scanItems` use two different pagination contracts:

- `scanItems` (`docs/agent-plans/2026-11-20-scan-items.md`) takes `exclusiveStartKey` and returns
  `lastEvaluatedKey`, as DynamoDB does. The public request and response contain only keys.
- `queryItems` takes `cursor` and returns `cursor`: an opaque string (`shared/query/cursor.ts`).

One database with two contracts is harder to learn, to document, and to wrap in a client library. FokosDB follows the
DynamoDB API, and DynamoDB uses keys for both `Query` and `Scan`. This RFC moves `queryItems` to keys.

The opaque cursor also gives less than its name says:

- It is base64url JSON. Any holder can decode it and read the hash key and the sort key.
- Its fingerprint (`computeCursorFingerprint`) is a hash with no secret. Any holder can build a valid cursor.
- `CURSOR_VERSION` makes each change of the format a compatibility question. A cursor from an older format fails with
  `cursor_version_unknown`.

### 1.2 What the reader must know about the current system

- **The request.** `QueryItemsOptions` (`shared/types.ts`) holds a list of sub-queries, `queries`. Each sub-query has
  a `hashKey`, an optional `sortKeyCondition`, and an optional `scanIndexForward`. Two sub-queries can have the same
  hash key.
- **The page.** `#queryItems` in `client/db.ts` runs the sub-queries in order, with one `partitions.range` call for
  each sub-query, until the page budget (`QueryPageBudget`, `shared/query/page-budget.ts`) stops the page. A sub-query
  of a promoted hash key reads more than one range partition. `walkRangeVisits` in `server/do-partition.ts` reads them
  in order, one visit for each partition.
- **The cursor today** is `{ v, d, fp, qi, inner: { hk, sk, incl } | null }`:

| Field | Meaning |
|---|---|
| `v` | `CURSOR_VERSION` |
| `d` | the direction of the sub-query `qi` |
| `fp` | the fingerprint of the queries, the filter, and the projection |
| `qi` | the index of the sub-query where the next page starts |
| `inner.hk`, `inner.sk` | the position inside sub-query `qi` |
| `inner.incl` | true: the next page starts at the position. False: strictly after it. |
| `inner: null` | the next page starts at the start of sub-query `qi` |

- **Where each resume position comes from.** The internal `ScanCursor` (`shared/partition/partition-store.ts`) is
  `{ hk, sk, inclusive }`. Four places make one:

| Stop | Code | Position |
|---|---|---|
| A. A budget reaches zero after a visit that read to its end | `walkRangeVisits` | `lastEvaluatedCursor`, exclusive |
| B. A budget rejects candidate `c` before the page takes it | `stopBefore` | `c`, inclusive |
| C. The visit limit stops the page at a visit boundary | `makeBoundaryCursor` | see below |
| D. The budget stops the page after a sub-query that read to its end | `#queryItems` | `inner: null` |

- `stopBefore` is in `createQueryPageCollector`.
- In stop C, the position is the end of the visit, inclusive, in ascending order. In descending order it is the start
  of the visit, exclusive.
- In stop D, `qi` is the next sub-query with a non-empty interval.

### 1.3 Glossary

- **Visit** — one range partition that `walkRangeVisits` reads for one sub-query. Its sort-key slice is
  `[visit.start, visit.end)`, projected on the hash key of the sub-query.
- **`L`** — the last evaluated row of the page.
- **Resume key** — the `lastEvaluatedKey` of a page. The next page starts strictly after it, in the direction of its
  sub-query.
- **`smallestValidAtOrAfter(b)`** — the smallest encoded sort key that is a valid public key and is at or after the
  bytes `b`. Section 4.2.7 of `docs/agent-plans/2026-11-20-scan-items.md` defines it for binary keys, string keys, and
  GSI sort keys.

## 2. Goals and requirements

### 2.1 In scope

- `queryItems` takes `exclusiveStartKey` in place of `cursor`, and returns `lastEvaluatedKey` in place of `cursor`.
- `db.gsi(indexName).queryItems` does the same, with the base keys in the key.
- A caller who reads all pages with `exclusiveStartKey = lastEvaluatedKey` gets each matching item once, in query
  order. No item is missed and no item appears twice.
- Each stop A to D has a resume key.
- The client validates a start key against its sub-query before any I/O.
- The `cursor` field, `shared/query/cursor.ts`, and the cursor validation codes are removed.

### 2.2 Out of scope

- **An encrypted or signed token.** A caller who must hide keys from its own users can wrap the key. A helper in
  `FokosDBStd` can do this later.
- **A change of the internal `ScanCursor`**, of the `range` dispatch shape, or of the read-through gate. The client
  turns a start key into an exclusive `ScanCursor`. The `inclusive` flag stays internal.
- **A change of `scanItems`.** Section 4.2.9 says what part of this design applies to it.

### 2.3 Requirements

- The public request and response must contain only keys and the index of a sub-query. They must contain no
  routingHash, no partition name, and no opaque token.
- A page must not cost more RPCs than today, except for the two cases in section 4.2.8.
- The change is breaking. FokosDB is pre-release, so this RFC has no migration of old cursors.
- This RFC must ship after `scanItems`. It uses `smallestValidAtOrAfter` from that RFC.

## 3. Milestones

Each milestone ships on its own, and the full test suite passes after each one.

1. **Stop facts in the RPC.** `QueryItemsRpcResponse` gets `stop` (section 4.2.3). `walkRangeVisits` and the
   collector fill it. The client does not read it yet, so there is no change in behaviour.
2. **Public keys.** `exclusiveStartKey` and `lastEvaluatedKey` replace `cursor` in `QueryItemsOptions` and
   `QueryItemsPage`. The client builds the resume key (section 4.2.5) and validates the start key (section 4.2.4).
   `shared/query/cursor.ts` and the cursor validation codes are removed.
3. **GSI.** `db.gsi(indexName).queryItems` takes and returns the GSI form of the key.
4. **Callers and documentation.** The `queryItems` paragraph of `AGENTS.md`, `examples/canary/src/workloads/query.ts`,
   and the tests that follow `cursor`.

## 4. Proposed solution

### 4.1 High-level overview

The cursor names a resume position in two ways: "at X" (`incl: true`) and "after X" (`incl: false`). A key can say
only "after K". The two are the same when no row that the page did not read is between K and X. So for each stop the
page returns a key K inside the part that it already read, with no unread row between K and the next unread row:

- **The page stopped between two rows.** K is `L`, the last row that the page evaluated. The next row after `L` in
  the read order is the first unread row. This covers stops A and B. When the rejected row is the first row of a
  partition in ascending order, K is `v'`, a valid key between the start of that partition and the row. The next
  page then goes to that partition directly.
- **The page stopped after a visit that held no row.** K is `v`, the smallest valid key at or after the start of that
  visit. The visit is empty, so "after `v`" skips no row. This covers stop C. The same rule is case 4 and case 5 of
  section 4.2.7 of the `scanItems` RFC.
- **The page stopped between two sub-queries.** K is `{ queryIndex }` with no position: the start of that sub-query.
  This covers stop D.

The direction comes from `scanIndexForward` of the sub-query. The fingerprint becomes a check in the client: the start
key must be inside the interval of its sub-query.

```
one ascending sub-query of a promoted hash key with three range partitions:

   R.0 [min, "g")      R.1 ["g", "p")      R.2 ["p", max)
   a  b  e             (empty)             q  r  t

page stop                                         lastEvaluatedKey   next page
B: the budget rejects "b"                         "a"  (L)           after "a": R.0 → b, e, ...
C: the visit limit stops the page after R.1       "g"  (v in R.1)    after "g": R.1, no row → R.2
B: the budget rejects "q", the first row of R.2   "p"  (v' < "q")    after "p": R.2 directly
```

### 4.2 Technical details

#### 4.2.1 Public API

```ts
type QueryStartKey = {
	/** The index in `queries` of the sub-query where the page starts. */
	queryIndex: number;
	/** The hash key of `queries[queryIndex]`. Absent: the page starts at the start of the sub-query. */
	hashKey?: HashKey;
	/** Absent with `hashKey`: the position before the first sort key of `hashKey`. */
	sortKey?: SortKey;
};

/** The start key of a GSI query. The base keys separate two entries with the same GSI keys. */
type GsiQueryStartKey = QueryStartKey & { baseHashKey?: HashKey; baseSortKey?: SortKey };

type QueryItemsOptions = {
	queries: Array<{ hashKey: HashKey; sortKeyCondition?: SortKeyCondition; scanIndexForward?: boolean }>;
	limit?: number;
	maxResponseBytes?: number;
	/** Replaces `cursor`. */
	exclusiveStartKey?: QueryStartKey;
	select?: QuerySelect;
	filter?: ConditionExpression;
	projection?: readonly ProjectionExpression[];
};

/** The options of `db.gsi(indexName).queryItems`. The start key can name one entry of the GSI. */
type GsiQueryItemsOptions = Omit<QueryItemsOptions, "exclusiveStartKey"> & {
	exclusiveStartKey?: GsiQueryStartKey;
};

/** `Key` is `QueryStartKey` for a table and `GsiQueryStartKey` for a GSI. */
type QueryItemsPage<Item, Key = QueryStartKey> = {
	items: Item[];
	count: number;
	scannedCount: number;
	/** Replaces `cursor`. Absent when every sub-query is read to its end. */
	lastEvaluatedKey?: Key;
	/** Unchanged. */
	meta: QueryItemsResult["meta"];
};

type GsiQueryItemsResult<T = never> = QueryItemsPage<GsiReadItem<T>, GsiQueryStartKey>;
```

- `FokosGsi.queryItems` takes `GsiQueryItemsOptions` and returns `GsiQueryItemsResult`. Its projected overload takes
  `GsiQueryItemsOptions & { projection; select?: "projection" }` and returns
  `QueryItemsPage<CallerType<T, ProjectedItem>, GsiQueryStartKey>`. This is the same pattern as `GsiScanItemsOptions`
  in the `scanItems` RFC.
- The table types `QueryItemsOptions`, `QueryItemsProjectedOptions`, `QueryItemsResult`, and
  `QueryItemsProjectedResult` use the default `Key`.
- `lastEvaluatedKey` can name a key that holds no item (sections 4.1 and 4.2.5). The caller must treat it as a resume
  value.
- A page can hold no item and still have a `lastEvaluatedKey`, as with the cursor today.
- A caller can send a different `filter`, `projection`, `select`, or `limit` on each page, as with `scanItems`.

#### 4.2.2 The position of a start key

The client turns a start key into the internal cursor of sub-query `queryIndex`:

| Start key | Internal cursor |
|---|---|
| `{ queryIndex }` | none: the sub-query starts at the start of its interval |
| `{ queryIndex, hashKey }` | `{ hk, sk: empty, inclusive: false }` |
| `{ queryIndex, hashKey, sortKey }` | `{ hk, sk: encode(sortKey), inclusive: false }` |
| GSI | `sk = gsiItemSortKey(sortKey, baseHashKey, baseSortKey)`, where an absent part is the empty key |

The cursor is always exclusive. "After `(hk, empty)`" in ascending order is the whole hash key, because no item has an
empty sort key. In descending order it is nothing, so the sub-query has no more rows. The client never returns
`{ queryIndex, hashKey }` with no `sortKey` for a descending sub-query (section 4.2.5).

#### 4.2.3 Stop facts in the RPC

The client builds the resume key, as `db.ts` does for `scanItems`. The partition reports the facts that the client
needs. `QueryItemsRpcResponse` keeps `lastEvaluatedCursor` and `nextCursor`, and gets one field:

```ts
type QueryStop =
	/** Stop A: resume after `lastEvaluatedCursor`. */
	| { kind: "after_last" }
	/** Stop B: a budget rejected `candidate` before the page took it. */
	| { kind: "before_candidate"; candidate: KeyBytes; visitStart: KeyBytes }
	/** Stop C: the visit limit stopped the page after the visit `[visitStart, visitEnd)`. */
	| { kind: "visit_limit"; visitStart: KeyBytes; visitEnd: KeyBytes | null };

type QueryItemsRpcResponse = {
	// ... the current fields
	/** Null when the sub-query is read to its end. */
	stop: QueryStop | null;
};
```

- `visitStart` is the start of the visit projected on the hash key of the sub-query. It is the empty key for the
  first visit, and for a visit whose start boundary is in an earlier hash key.
- `visitEnd` is the end of the visit projected the same way. Null means that the visit has no end.
- `nextCursor` stays. The client needs it for the extra visit of section 4.2.5.
- `QueryItemsRpcRequest` does not change.

**`L` in the visit.** The resume rules must know if `L` is in the visit of the stop. The RPC does not send this fact,
because the client can find it from `lastEvaluatedCursor` and the visit edges:

| Direction | `L` is in the visit when |
|---|---|
| ascending | `L !== null && L.sk >= visitStart` |
| descending | `L !== null && (visitEnd === null \|\| L.sk < visitEnd)` |

The rule is correct for these reasons:

- `walkRangeVisits` reads the visits of one call in storage order. The visits do not overlap. In ascending order each
  earlier visit is below `visitStart`. In descending order each earlier visit is at or above `visitEnd`.
- `lastEvaluatedCursor` is the last row that any visit of the call evaluated. A rejected candidate does not set it.
- Descending stop B has no `visitEnd`, and does not need it. Each descending stop B returns `key(L)`
  (section 4.2.5).

`L` must be the `lastEvaluatedCursor` of the call for sub-query `i`, not the last row of the page. Two sub-queries can
have the same hash key. A row of an earlier sub-query can then pass the comparison and give a wrong result. For the
extra visit of section 4.2.5, `L` is the `lastEvaluatedCursor` of that second call.

#### 4.2.4 Validation

The client validates `exclusiveStartKey` before any I/O:

- `queryIndex` must be an integer from 0 to `queries.length - 1`.
- `sortKey` needs `hashKey`. On a GSI, `baseHashKey` needs `hashKey`, and `baseSortKey` needs `baseHashKey`.
- Each key part gets the checks of `validateItemKeys`.
- `hashKey` must equal `queries[queryIndex].hashKey`, as encoded bytes.
- The encoded sort key of the position must be inside the closed range `[lower.value, upper.value]` of the
  normalized interval of the sub-query (`normalizeSkInterval`). An absent bound has no limit. The check accepts a bound
  value itself, also for an exclusive bound. "After `lower.value`" in ascending order is the start of the interval, and
  "before `upper.value`" in descending order is the start of a descending interval.

The new codes are `query_start_key_invalid` for the first four rules and `query_start_key_outside_query` for the last.
`cursor_malformed`, `cursor_version_unknown`, `cursor_query_index_out_of_range`, `cursor_direction_mismatch`, and
`cursor_fingerprint_mismatch` are removed.

#### 4.2.5 The resume key

`resumeKey(stop, L, query, i)` is a pure function in the client. `i` is the index of the sub-query that stopped. `lo`
is `lower.value` of the interval of the sub-query, or the empty key.

Both stop B and stop C use one candidate key: `v = smallestValidAtOrAfter(max(visitStart, lo))`.

| Stop | Condition | Resume key |
|---|---|---|
| A, `after_last` | none | `key(L)` |
| B, `before_candidate` | `L` in the visit | `key(L)` |
| B, `before_candidate` | `L` not in the visit, ascending, `v < candidate` | `v`, named `v'` in section 4.2.6 |
| B, `before_candidate` | else | `key(L)` |
| C, `visit_limit` | `L` in the visit | `key(L)` |
| C, `visit_limit` | `L` not in the visit, `v` inside the visit and the interval | `v` |
| C, `visit_limit` | else, the page has `L` | `key(L)` |
| C, `visit_limit` | else | one more visit (below) |
| D | the budget stopped the page after sub-query `i` | `{ queryIndex: j }` |

In stop D, `j` is the next sub-query with a non-empty interval.

- "`L` in the visit" is the comparison of section 4.2.3.
- `key(L)` is `{ queryIndex, hashKey, sortKey? }` of `L`, with the index of the sub-query of `L`. On a GSI, it also has
  `baseHashKey` and `baseSortKey` (`gsiKeys`).
- A key with an empty sort key has no `sortKey`.
- **`L` exists for stop B.** A budget can reject a candidate only after the page took a row. Before that,
  `allowOversizedFirstItem` holds, the evaluated-item budget is at least 1, and the evaluated-byte budget is larger
  than any item.
- **`v` is inside the visit.** Each range partition held one item or more when its split created it, and the split
  puts each boundary between two items (`planBucketBoundaries`). So a valid key exists in the visit, and `v` is at or
  below it.
- **`v` and `v'` skip no row.** `v` is in a visit that evaluated no row, because `L` is not in it. `v'` is below
  `candidate`, the first row of its visit. So no row is between `v'` and `candidate`.
- **One more visit.** When no rule gives a key, the client calls `partitions.range` again for sub-query `i`, with
  `nextCursor` and a visit budget of 1. That is the internal cursor of stop C. Then it applies the table again. This
  happens only when `v` is outside the interval and the page has no `L`. That can happen only if the interval inside
  the visit holds no valid key. The extra visit can be visit 101 of the page. This is permitted: the limit bounds the
  fan-out of one page, and one more visit in a rare case does not change that bound.

#### 4.2.6 The fix for stop B at the first row of a visit

Without the `v'` rule, stop B at the first row of visit Y returns `key(L)` in an earlier visit X. The next page then
visits X again, finds no row, and moves to Y. The cursor today goes to Y directly.

`v'` avoids this. It is at or after `visitStart`, so its owner is Y. It is below `candidate`, so "after `v'`" starts at
`candidate`. When `v' = candidate`, `v'` cannot be used, because "after `v'`" would skip the row. This happens when
the start boundary of Y is the full key of `candidate`. The page then returns `key(L)`.

`v'` applies to the ascending direction only. A descending page that stops at the first row of a visit returns
`key(L)`, and the next page pays one extra RPC (section 4.2.8). The reasons:

- In descending order the first row of visit Y is its largest row, and "after K" means "below K". So K must be a
  valid key in `(candidate, visitEnd)`. That is the smallest valid key strictly above `candidate`.
- `smallestValidAtOrAfter` does not give that key. It completes the cut last character of a boundary prefix, so its
  result is never longer than an item key. The key strictly above `candidate` is `candidate` with one more character.
  That needs a new function with more failure cases:
  - When `candidate` is at `maxSortKeyBytes`, no longer key exists.
  - The smallest character to add depends on the sort-key type and on the characters that a sort key can hold.
  - On a GSI, the key must go through the escaped layout of `gsiItemSortKey`. Each of `sortKey`, `baseHashKey`, and
    `baseSortKey` can be the part that grows.
- The page stop is rare. It needs a descending sub-query, a promoted hash key, and a budget that stops at the
  first row of a partition. The cost is one RPC that reads no row.

A later change can add the descending rule if a measurement shows that the extra RPC matters.

#### 4.2.7 GSI

A GSI query runs the same code with the GSI client (`#gsiClient`).

- `visitStart`, `candidate`, and `lo` are physical GSI sort keys.
- `smallestValidAtOrAfter` uses its GSI form, from section 4.2.9 of the `scanItems` RFC.
- The client turns the result into `sortKey`, `baseHashKey`, and `baseSortKey`. An absent part is a part that the
  result does not reach.
- `gsiItemSortKey` of those parts gives the same position (section 4.2.2).

#### 4.2.8 Performance

A key resumes after a row or a boundary. A cursor can also resume at one. So when a page ends at the last row of a
visit, the next page can visit that partition again and read nothing. The cost is one RPC, 10 to 100 ms within one
location hint or jurisdiction.

| Stop | Cursor today | Keys | Extra RPC |
|---|---|---|---|
| A at the end of visit X | after `L`: visits X again | after `L`: visits X again | none |
| B in the middle of a visit | at `c` | after `L`, in the same visit | none |
| B at the first row of Y, ascending | at `c`: Y directly | after `v'`: Y directly | none, except when `v' = c` |
| B at the first row of Y, descending | at `c`: Y directly | after `L`: X again | one |
| C after visit E | at the boundary: the next visit | after `L` or `v`: E again | one, after 100 visits |
| D between two sub-queries | the next sub-query | the next sub-query | none |

- Each extra RPC occurs only for a promoted hash key. A hash key that is not promoted is in one partition, so a page
  never stops at a visit boundary of it.
- The row reads are the same. The extra RPC is one index seek that returns no row.
- The client check of section 4.2.4 costs no I/O. The fingerprint, which hashed all sub-queries on each page, is
  removed.

#### 4.2.9 `scanItems`

`scanItems` already uses keys. Stop B has one case in `scanItems` too: a step that evaluated no row, because the
remaining response bytes cannot hold its first row. That page returns a boundary key in the slice before, so the next
page visits that slice again. A key in `[S, c)` inside the new slice can avoid it. For a hash slice this needs a found
key whose routingHash is between `S` and the routingHash of `c`, so it is more complex than `v'`. That page stop is
rare. This RFC does not change `scanItems`.

#### 4.2.10 Testing

- **Full read.** A property test with random items, random sub-queries in both directions, random budgets, and a
  range tree with several partitions. The test reads all pages with `exclusiveStartKey = lastEvaluatedKey`. The
  result must be each matching item once, in the order of the sub-queries and of their directions. The test computes
  the expected list from the items that it wrote. It does not compare with the cursor, so it does not depend on the
  order of the milestones.
- **Each stop.** One test for each row of the table of section 4.2.5, in both directions, including `v' = candidate`
  and a visit with an empty `v`.
- **`L` in the visit.** Two sub-queries on the same hash key, ascending. The page reads rows of sub-query 0, and then
  stops by stop B at the first row of a later visit of sub-query 1. The resume key is `v'`, not `key(L)`. A second
  case stops by stop C in descending order, with `L` in an earlier visit at or above `visitEnd`.
- **GSI types.** A type test: `db.gsi(indexName).queryItems` accepts a `GsiQueryStartKey`, and its `lastEvaluatedKey`
  has the type `GsiQueryStartKey`.
- **Splits between pages.** A range split and a promotion between two pages give no duplicate and no missed item.
  Read `test/property-based/README.md` first. The property suites `query-items-split.test.ts` and
  `query-items-active-split.test.ts` move to keys.
- **Validation.** Each rule of section 4.2.4, with its code. A start key at an exclusive bound value is accepted.
- **GSI.** Two entries with the same GSI keys and different base keys, with a page stop between them.
- **RPC count.** A page of a promoted hash key that stops by rule `v'` makes no extra RPC on the next page.

## 5. Alternative options

- **Keep the opaque cursor for `queryItems`.** Rejected: two pagination contracts in one API. The cursor also hides
  nothing and protects nothing (section 1.1), and it needs a version.
- **An encrypted and signed cursor.** It hides the keys and rejects a changed token. Rejected for the core API: it
  needs a secret for each deployment, and the caller cannot build or store a start key. DynamoDB users do this on top
  of the key when they need it ([dynamodb-paginator](https://github.com/supersoniko/dynamodb-paginator),
  [dyno](https://pkg.go.dev/github.com/RealImage/dyno)). A `FokosDBStd` helper can do the same.
- **No `queryIndex`, with the hash key alone.** Rejected: two sub-queries can have the same hash key.
- **The predecessor of the boundary for stop C.** Keys have a maximum length, so a predecessor exists. Rejected: it is
  a key of up to `maxSortKeyBytes` bytes, filled with U+10FFFF, in the public response.
- **`v' = smallestValidAtOrAfter(visitEnd)` for stop C.** It would resume in the next visit directly. Rejected: the
  page did not read that visit, and `v'` can be a real row of it. "After `v'`" would then skip that row.
- **Resume at the start of sub-query `i` for stop B.** It is correct, but it visits again each empty partition of
  sub-query `i` before `candidate`. `key(L)` and `v'` visit at most one.

## 6. Frequently asked questions

**Why is the inclusive flag not necessary?**
"At X" and "after K" give the same next row when no unread row is between K and X. For stop B, K is `L` and X is the
next row after `L`. For stop C, K is `v` in an empty visit. Section 4.1 gives the rules.

**What happens when a caller changes `scanIndexForward` between pages?**
The page resumes from the key in the new direction. The cursor rejected this with `cursor_direction_mismatch`. A key
cannot detect it. The result is the rows on the other side of the key, as for any start key that the caller chooses.

**What happens when a caller changes `queries` between pages?**
The check of section 4.2.4 rejects a start key whose hash key is not the hash key of `queries[queryIndex]`, or whose
sort key is outside its interval. A change that keeps the key inside the interval resumes from the key, which is
correct for the new request. DynamoDB also rejects a start key that does not match the sort-key condition of the
query, with a `ValidationException`
([Qiita](https://qiita.com/qwe001/items/aa2ab0a6462d4f48e45d)).

**What happens to a cursor from an earlier version?**
The request type has no `cursor` field. FokosDB is pre-release, and this change is breaking.

**Is an empty page with a `lastEvaluatedKey` still possible?**
Yes, as with the cursor today and with DynamoDB
([AWS docs](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.Pagination.html),
[re:Post](https://repost.aws/questions/QUTIBbXECOT3KpKWIZH-kuRg/dynamodb-querying-gsi-page-lastevaluatedkey-returns-non-null-non-empty-key-when-there-are-no-more-items-to-be-retrieved)).
A filter or a stop at a boundary can give a page with no item.

## 7. References

- `AGENTS.md`
- `docs/agent-plans/2026-11-20-scan-items.md`
- `docs/agent-plans/query-items-design.md`
- `docs/agent-plans/2026-09-11-query-items-selection.md`
- `docs/agent-plans/2026-09-14-read-projections-and-query-filters.md`
- [DynamoDB Developer Guide: Paginating table query results](
  https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.Pagination.html)
- [Qiita: a DynamoDB start key that does not match the sort-key condition](
  https://qiita.com/qwe001/items/aa2ab0a6462d4f48e45d)
