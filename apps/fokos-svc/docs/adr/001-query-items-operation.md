# ADR 001: queryItems Operation

Date: 2026-06-20

## Status

Accepted

## Context

FokosDB is a DynamoDB-inspired key-value store built on Cloudflare Durable Objects. Each item is addressed by a `(hashKey, sortKey?)` pair, where the hash key determines which partition owns the item and the sort key determines its position within that partition.

Before this change, FokosDB supported point operations only: `putItem`, `getItem`, `deleteItem`, and transactional variants. Users had no way to retrieve a range of items by sort key without knowing every key in advance.

The system's partition topology adds complexity to range scanning. A single hash key's items may live on a single hash-leaf partition, or they may be spread across a tree of range partitions if the key was promoted due to size. Any query operation must transparently traverse this topology and return a consistent, paginated stream regardless of how the data is physically distributed.

## Decision

We implement a `queryItems` operation that accepts one or more sub-queries, each specifying a hash key and an optional sort-key condition. The operation returns items grouped by sub-query in request order, sorted by sort key within each group, with opaque cursor-based pagination.

### Request model

The caller provides a list of sub-queries rather than a single key condition:

```typescript
queryItems({
	queries: [
		{ hashKey: "user-alice", sort: { op: "begins_with", prefix: "order#" } },
		{ hashKey: "user-bob", sort: { op: "between", lower: "2024-01", upper: "2024-12" }, scanIndexForward: false },
	],
	limit: 100, // optional item count cap
	maxPageBytes: 1_000_000, // optional byte budget override
	cursor: "...", // opaque continuation token from a previous page
});
```

Each sub-query is independent: results from `user-alice` appear first (in sort-key order), followed by `user-bob`. The multi-sub-query model avoids N separate round-trips for the common pattern of fetching related data across several hash keys.

Scan direction (`scanIndexForward`, defaulting to `true`) is per-query, not global. Since each sub-query is an independent scan with its own hash key, interval, and cursor resume position, there is no reason to constrain all sub-queries to the same direction. A caller can fetch one key's items in ascending order and another's in descending order within the same request.

Duplicate hash keys are allowed. Repeating a hash key with disjoint sort conditions is the escape hatch for OR-style queries within a single key (e.g. "items starting with `order#` OR items starting with `invoice#`"). Deduplication is the caller's responsibility.

### Sort-key conditions

Eight operators cover the DynamoDB condition set plus a general-purpose `range`:

| Operator                 | Meaning                                                                  |
| ------------------------ | ------------------------------------------------------------------------ |
| `eq`                     | Exact match on sort key                                                  |
| `gt`, `gte`, `lt`, `lte` | Open or half-open range                                                  |
| `between`                | Inclusive range `[lower, upper]`                                         |
| `begins_with`            | Prefix match                                                             |
| `range`                  | Fully explicit: optional lower/upper, each with its own inclusivity flag |

Every operator is normalized into a single canonical byte-space interval before anything else runs:

```typescript
type SkInterval = {
	lower?: { value: KeyBytes; inclusive: boolean };
	upper?: { value: KeyBytes; inclusive: boolean };
};
```

An absent end means unbounded in that direction. `null` (not `{}`) represents an unsatisfiable interval (e.g. `between "z" "a"`), which is silently skipped rather than errored.

The `begins_with` operator synthesizes its upper bound using `KeyCodec.successor(prefix)`, which increments the last byte below `0xFF` and drops trailing bytes. If the prefix is entirely `0xFF` bytes, there is no successor and the upper bound is left unbounded. An empty prefix normalizes to match-all.

### End-to-end flow

The operation flows through four layers:

1. **FokosDB orchestrator** (`queryItems` method on the main class)
   - Validates inputs, encodes keys into `KeyBytes`, normalizes each sub-query's sort condition into an `SkInterval`.
   - Computes a cursor fingerprint over the query list (keys, intervals, and per-query direction — not limit or byte budget, which may change between pages).
   - Allocates a shared `PageBudget` (byte budget, optional item limit, partition visit cap).
   - Iterates sub-queries in order starting from the cursor's resume point, making one RPC per sub-query to the partition that owns the hash key.
   - Accumulates items and metadata, decrementing the shared budget after each RPC.
   - Stops when a sub-query returns a mid-scan cursor, or the budget is exhausted, or all sub-queries are drained.

2. **PartitionDO** (the Durable Object that receives each sub-query RPC)
   - If this is a hash-leaf partition (the common case), scans its local SQLite `items` table directly.
   - If the hash key has been promoted to a range tree, delegates to the range-tree walker.
   - Split-forwarding and bloom-filter-based promotion detection are transparent to the caller.

3. **Range-tree walker** (`walkRangeChildren`)
   - Iterates child partitions in boundary order (ascending or descending depending on scan direction).
   - For each child: checks intersection with the query interval, skips children fully before the cursor, clips the interval to the child's `[start, end)` ownership range, and forwards the RPC with the remaining budget.
   - Stops when a child returns a mid-scan cursor, or the byte/item budget is exhausted, or the partition visit cap is reached.

4. **Leaf partition scan** (`collectBatch` + `queryRangeItemsPage`)
   - `queryRangeItemsPage` builds a SQL query against the `items` table with bound-inclusive/exclusive conditions per direction and cursor state.
   - `collectBatch` wraps this in a paginated loop with byte-budget and item-count tracking.

### Pagination

Pagination uses three independent budget dimensions:

- **Byte budget**: cumulative estimated row size across all scanned items. Defaults to 3 MB, capped at 16 MB server-side. The first matched item is always included even if it alone exceeds the budget, preventing stalls on single large rows.
- **Item count limit**: optional per-request cap. When present, the page stops after returning that many items.
- **Partition visit cap**: hard-coded at 100 leaf partitions per page. Bounds the cross-DO subrequest fan-out over heavily-split range trees. Well under the Cloudflare Workers per-request subrequest limit.

When any budget is exhausted, the operation emits a continuation cursor and stops. The cursor is an opaque base64url-encoded JSON token containing:

- A version number (for forward compatibility).
- The scan direction of the resumed sub-query (`fwd` / `rev`).
- A fingerprint binding the cursor to the original request (queries, intervals, and per-query directions). Changing any part of the request between pages is rejected.
- The sub-query index (`queryIdx`) to resume at.
- An optional inner resume position (`hashKey`, `sortKey`, `inclusive` flag) within that sub-query.

The cursor is logical, not physical. It encodes a position in sort-key space, not a reference to a specific Durable Object. This makes cursors stable across partition splits and promotions: the routing layer re-resolves the position through the current topology.

### Boundary cursors vs row cursors

Two cursor shapes arise from different stop conditions:

- **Row cursor**: emitted when the byte budget or item limit runs out mid-scan. Points at the last returned row with `inclusive: false` (resume strictly after it). This is the common case.
- **Boundary cursor**: emitted when the partition visit cap is reached mid-walk across a range tree. Points at the boundary between the last-visited child and the next unvisited child. For ascending scans: `inclusive: true` at the next child's start boundary. For descending scans: `inclusive: false` at the current child's start boundary.

The distinction matters because boundary cursors must include or exclude the boundary key itself depending on which child owns it. The `isOriginalInclusiveCursor` check uses value equality (`KeyCodec.compare`) rather than reference identity, making it safe across the structured-clone boundaries that Cloudflare DO RPCs use.

### Descending scans

When `scanIndexForward` is `false`:

- The SQL query uses `ORDER BY sk DESC` with the near-bound and far-bound swapped (cursor or upper bound first, lower bound second).
- The range-tree walker iterates children in descending boundary order.
- Boundary cursors point below the current child's start (exclusive), because that boundary key belongs to the already-visited child.
- The lower-bound SQL condition is skipped when it would be the zero-length sentinel with `inclusive: true`, since that matches all keys and adds nothing to the query.

### Key encoding

All key comparison, cursor arithmetic, and interval clipping operates in encoded byte space (`KeyBytes`). The encoding is:

- **Strings**: UTF-8 bytes (no tag).
- **Binary** (`Uint8Array`): `0xFF` prefix byte followed by raw bytes. Since UTF-8 never contains `0xFF`, the tag is unambiguous without escaping.
- **Absent sort key**: zero-length `Uint8Array` (the global byte minimum, sorts before everything).

Byte-order comparison (`memcmp` semantics) matches the SQLite `BLOB` column ordering, so the JS-side interval arithmetic and the SQL-side `WHERE` clauses agree on key order.

## Consequences

### What this enables

- Range queries across sort keys with DynamoDB-compatible condition semantics.
- Multi-key fan-out in a single request, avoiding N round-trips for related hash keys.
- Transparent pagination over promoted range trees with bounded subrequest fan-out.
- Stable cursors that survive partition splits and key promotions.

### Limitations and deferred work

- **No `countOnly` mode**: a `SELECT COUNT(*)` path that avoids materializing rows is deferred. The plumbing (the field exists in the options type) is in place but the scan path always materializes items.
- **No filter expressions**: only key conditions are supported. There is no `FilterExpression` or `ProjectionExpression` equivalent; the `data` field is returned as-is.
- **Binary keys over HTTP**: the HTTP API surface currently accepts only string keys. Supporting `Uint8Array` keys over HTTP would require a `keyEncoding` discriminator in the wire format.
- **No cross-partition snapshot isolation**: a multi-leaf result is not a global point-in-time snapshot. Items may change between leaf visits within a single page. Consistent multi-key reads require `transactGetItems`.
- **Partition visit cap is not user-configurable**: the 100-partition cap is hard-coded. A warning is logged when the cap is reached. This is conservative but sufficient for current workloads; it can be raised or made configurable later.
- **No retries on transient DO failures**: if a partition RPC fails mid-page, the error propagates to the caller. Retry logic with cursor-based resumption is deferred.
