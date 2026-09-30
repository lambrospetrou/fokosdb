# RFC — Separate item-size facts from storage estimates and correct range splits

**State:** Draft
**Date:** 2026-09-30

## Table of contents

- [1. Overview and context](#1-overview-and-context)
- [2. Goals and requirements](#2-goals-and-requirements)
- [3. Timeline and milestones](#3-timeline-and-milestones)
- [4. Proposed solution](#4-proposed-solution)
  - [4.1 High-level overview](#41-high-level-overview)
  - [4.2 Technical details](#42-technical-details)
  - [4.3 Open questions](#43-open-questions)
- [5. Alternative options](#5-alternative-options)
- [6. Frequently asked questions](#6-frequently-asked-questions)
- [7. References](#7-references)

## 1. Overview and context

`PartitionStore` stores committed items in SQLite. A hash key can have multiple items, ordered by sort key.
A range split divides that order into contiguous children. The planner uses item sizes to select boundaries.

### 1.1 Current size accounting

`shared/partition/item-size.ts` defines the current formula:

```text
est_row_bytes =
    octet_length(stored data)
  + octet_length(encoded hash key)
  + octet_length(encoded sort key)
  + 108
```

SQLite measures the variable lengths exactly. Text uses UTF-8 bytes. Binary data uses BLOB bytes.
JSON uses the stored JSONB bytes, not the original JSON text. Binary keys include their one-byte type tag.
An absent sort key has zero encoded bytes.

The constant `EST_ROW_BYTES_K` estimates fixed storage overhead. It does not measure physical storage.

`items.est_row_bytes` is an ordinary column. `idx_items_scan` contains `(hk, sk, est_row_bytes)`.
The range scan reads this covering index without reading item data.
The existing query-plan tests protect this property.

`key_size_estimates.est_bytes` holds the sum for one hash key.
An insert adds its size. An overwrite adds the difference between its new and old sizes.
A delete subtracts the stored size. The TTL sweep subtracts sizes by hash key.
Migration adds only newly inserted rows, grouped by hash key per page.
A retried migration page does not count an existing row twice.

This accounting is consistent under the current formula. However, the formula serves different purposes.

### 1.2 The formula misses variable index costs

The `items` table stores each encoded key. Two indexes also store those keys:

- The index for `UNIQUE(hk, sk)`.
- `idx_items_scan`.

An item with a non-null TTL also has an entry in `idx_items_ttl`.
SQLite index entries contain their indexed columns and the rowid.
The SQLite file-format reference describes this layout.

The current formula counts key bytes once. A fixed overhead cannot account for variable key lengths in other copies.
For 1,024 combined key bytes and 100 data bytes:

```text
Current estimate:           100 + 1,024 + 108 = 1,232 bytes
Data and three key copies:  100 + 3 * 1,024   = 3,172 bytes
```

The second value excludes record headers, rowids, and page overhead. It is not a physical storage measurement.
Variable sort-key lengths can therefore affect range balance. Long keys also reduce the accuracy of promotion estimates.

### 1.3 Storage estimates also control item validity

The same formula supplies the `MAX_ITEM_BYTES` guard, transaction prepare checks, and update probes.
Query scans also use it for the evaluated-byte budget.

A change to storage overhead changes which item payloads the API accepts.
An increase can invalidate an item that a transaction already accepted during prepare.
A prepared transaction must commit, so adjustable storage costs must not control that decision.

Changing the constant also leaves old estimates in existing rows.
New writes and migration imports use the new constant. Untouched rows retain the old constant.
The per-key total can remain internally consistent while its rows use different storage models.

### 1.4 The range planner has two defects

`PartitionStore.planRangeSplit` adds the crossing row's bytes before it creates a boundary.
That boundary assigns the crossing row to the upper child.
The cumulative total therefore includes bytes that the lower child does not receive.

```text
Item weights:          [111, 111, 111, 111]
Requested children:    2
Target bytes:          222
Current child weights: [111, 333]
Possible balance:      [222, 222]
```

The planner also advances the next threshold with `threshold = acc + step`.
A dominant item can push a later threshold beyond the total.

```text
Item weights:       [111, 5111, 111]
Requested children: 3
Current result:     skewed_bytes
Valid child weights: [111, 5111, 111]
```

Three non-empty children are possible. Equal byte balance is not possible because an item is indivisible.
The existing test expects `null` for this case. That test preserves the defect instead of detecting it.

This RFC changes the range-floor rule in `2026-09-27-over-size-split-trigger.md`.
It also replaces the threshold loop specified in `range-split-boundary-byte-seek.md`.

### 1.5 Why this change is needed now

The item-limit rule and persisted size fields need separate meanings before production use.
Future tables and indexes must permit changes to storage estimates without changes to item validity.
The range planner must distinguish an impossible split from an imperfect byte balance.

## 2. Goals and requirements

### 2.1 In scope

- Separate exact item bytes, estimated storage bytes, and estimated RPC response bytes.
- Replace persisted adjustable item estimates with exact size facts.
- Maintain exact per-key aggregates for those facts.
- Account for key copies in the existing indexes, including the partial TTL index.
- Permit coefficient changes without a rewrite of every item.
- Correct boundary byte accounting and permit splits with dominant items.
- Specify the compatibility changes, migration constraints, and required tests.

### 2.2 Out of scope

- Exact attribution of SQLite pages to individual items. Pages contain shared and variable overhead.
- A generic cost plug-in framework. Future features add facts only when their tables need them.
- New supporting tables or indexes for application features.
- A redesign of transaction storage or recovery. Temporary transaction storage stays separate from committed-item facts.
- Coordinator size accounting. This RFC concerns the partition item model and range planner.
- Changes to routing names, key encoding, or ownership rules.
- Changes to the public configuration surface for storage coefficients.
- Replacement of the streaming scan with histograms, window functions, or whole-partition materialization.

### 2.3 Requirements

1. The item limit must use a stable encoded-item definition, independent of storage coefficients.
2. Prepare and commit must apply the same item-validity rule.
3. Query evaluated bytes must use exact item bytes. Response budgets must retain their separate RPC estimates.
4. The per-key estimate must equal the sum of row estimates under the same model.
5. Every path that changes item facts must update the active summary in the same storage transaction.
6. A migration retry must contribute facts only for rows that it inserts.
7. The planner must read a complete owned slice and its summary in one `transactionSync` snapshot.
8. With at least `N` distinct items, the planner must produce `N` non-empty children.
9. Each boundary must separate adjacent keys in canonical byte order.
10. Storage-model changes must not reprice old items only when later writes touch them.
11. Size and count scans must remain index-only. They must not read item data for size accounting.
12. The physical split trigger and admission guard must continue to use `sql.databaseSize`.
13. No test can need a new production hook.

## 3. Timeline and milestones

TODO: Supply the implementation order and reviewable milestones. No delivery dates are specified.

## 4. Proposed solution

### 4.1 High-level overview

Store encoded item bytes as a fact, without adjustable overhead.
Maintain byte totals and row counts per hash key.
Calculate storage estimates from these facts with one shared model.

The storage model accounts for repeated keys and fixed overhead.
A later coefficient change uses the same facts for existing and new items.
A future supporting table adds byte totals or row counts only when its actual shape needs them.

The range planner selects feasible boundaries near byte targets.
It measures bytes at the actual boundary and reserves an item for each remaining child.
A dominant item can produce an unequal split, but it does not prevent a valid split.

```text
Stored representation
        |
        v
Exact item facts ---------> item limit and query evaluated-byte budget
        |
        +-----------------> exact per-key aggregates
        |                              |
        v                              v
One storage model          One storage model
        |                              |
        v                              v
Row weights                key estimate and item count
        |                              |
        +-------------> range planner <-+
                                       |
                                       +--> promotion checks

Materialized RPC data -----> response estimate -----> RPC page budget
sql.databaseSize ----------> physical split trigger and admission guard
```

### 4.2 Technical details

#### 4.2.1 Size definitions

| Value                      | Definition                                  | Consumers                         |
| -------------------------- | ------------------------------------------- | --------------------------------- |
| `item_bytes`               | Stored encoded data plus encoded keys       | Item limit and query evaluation   |
| `estimated_storage_bytes`  | Item facts priced by the active model       | Range boundaries and promotion    |
| `estimated_response_bytes` | Estimated serialized RPC payload            | Response and migration page sizes |

The exact item formula is:

```text
key_bytes  = octet_length(hk) + octet_length(sk)
item_bytes = octet_length(stored data) + key_bytes
```

This definition retains UTF-8, BLOB, JSONB, and canonical key measurements from section 1.1.
It excludes item metadata, indexes, supporting tables, and adjustable overhead.
The `MAX_ITEM_BYTES` value stays unchanged; the bytes charged to it change.

This definition is FokosDB's encoded-item rule. It does not claim exact DynamoDB size compatibility.

`estimateItemBytes()` and `estimateProjectedRowBytes()` remain RPC response estimators.
They must not supply row weights or per-key aggregates.
Migration response estimates continue to apply to the raw representation that migration sends.

#### 4.2.2 Item schema and covering index

Replace `items.est_row_bytes` with an ordinary `item_bytes` column.
Each writer must calculate it with SQLite over the same expression that produces the stored data.

Keep the `items` table as a rowid table. Keep `item_id` and its migration rules unchanged.
Keep the unique item-key constraint and the partial TTL index.

The scan index must cover:

- `hk` and `sk`.
- `item_bytes`.
- The TTL status needed by the storage model.

The old-facts read on a write path must also use this covering index.
The index must permit a range scan to calculate key lengths without reading item data.

Do not replace the ordinary size column with a generated column.
The existing Workers query-plan tests show why this code needs an ordinary column for index-only scans.

#### 4.2.3 Per-key facts

Replace the persisted `est_bytes` total with exact aggregates in the per-key summary.
The table below defines the facts; it does not decide a new table name.

| Fact             | Meaning                                              |
| ---------------- | ---------------------------------------------------- |
| `item_count`     | Number of committed items represented by the summary |
| `item_bytes`     | Sum of exact encoded item bytes                      |
| `key_bytes`      | Sum of combined encoded key bytes                    |
| `ttl_item_count` | Number of those items with a non-null TTL             |
| `ttl_key_bytes`  | Sum of combined key bytes for items with a TTL        |

A TTL contributes when it is non-null, including before an expired item is swept.
The facts describe the stored rows, not a query's visible rows.

The equality rules apply to an active summary for a complete owned leaf slice.
An import has partial facts until its data is complete.
A source can retain copies after ownership moves. Its summary follows the existing completion and cleanup lifecycle.
Those copies must not become a new owned slice for the range planner.

The summary remains keyed by `hk`.
A single-key total lookup reads one summary row, without an item scan.

#### 4.2.4 Storage model

Use the following model for one row:

```text
row_storage_estimate =
    item_bytes
  + 2 * key_bytes
  + base_overhead
  + (has_ttl ? key_bytes + ttl_overhead : 0)
```

Use its aggregate form for one hash key:

```text
key_storage_estimate =
    item_bytes
  + 2 * key_bytes
  + item_count * base_overhead
  + ttl_key_bytes
  + ttl_item_count * ttl_overhead
```

The item bytes already contain the table's key copy.
The factor of two adds the copies in the unique index and the scan index.
The TTL term adds the key copy and fixed cost in the partial TTL index.

`base_overhead` estimates fixed costs of the item row and its always-present indexes.
`ttl_overhead` estimates the remaining fixed cost of the TTL index entry.
Their initial values remain open in section 4.3.1.
The existing 108 bytes are context, not a measured value for the revised schema.

These estimates do not equal physical database size.
They omit shared partition state and temporary transaction storage.
Page occupancy, overflow pages, and rowid widths can also affect physical size.

Define the row and aggregate expressions in one shared model.
The expression must use exact facts rather than coefficients stored in each item.

#### 4.2.5 Indexed promotion lookup and model changes

`largestKeysAtLeast` must retain an indexed threshold lookup and descending size order.
The lookup must use the same model as direct per-key estimates and range weights.

An expression index over the per-key facts is one option.
A coefficient change then rebuilds the summary index, not every item.
The index strategy and activation procedure remain open in section 4.3.3.

A deployment must not use an old index with a new estimate expression.
It must not expose a partially rebuilt summary as a complete one.
The model and the summary access path must agree after restart and recovery.

A coefficient change needs no rewrite of `items.item_bytes` or the exact aggregate facts.
It can still need schema or index work. That work must account for the number of distinct hash keys.

#### 4.2.6 Fact updates and atomicity

The caller continues to compose atomic operations with `PartitionStore.transactionSync`.

| Operation                  | Required fact change                                            |
| -------------------------- | --------------------------------------------------------------- |
| Insert                     | Add the new row facts and increment `item_count`                 |
| Overwrite or update        | Apply new facts minus old facts; keep `item_count` unchanged     |
| Delete of an existing item | Subtract the old row facts and decrement `item_count`            |
| Delete of an absent item   | Change no item facts                                            |
| TTL sweep                  | Subtract facts for each deleted row, grouped by hash key         |
| Migration insert           | Add facts only when `insertItemIfAbsent` inserts the row         |
| Migration retry            | Add no facts for an existing row                                |
| Source copy cleanup        | Preserve the existing moved-key summary and copy lifecycle       |
| Metadata-only mutation     | Keep encoded byte facts unchanged unless a modeled fact changes |

A change from null TTL to non-null TTL adds the row to both TTL aggregates.
The reverse change subtracts it. A TTL timestamp change between non-null values leaves those aggregates unchanged.

Migration keeps the existing per-page grouping by hash key.
It must not introduce one summary write per imported item.
Do not replace the bulk paths with item-table triggers.

Pending rows must not contribute committed-item facts during prepare.
Commit changes those facts when it applies the committed item.
Cancel changes no committed-item facts.

#### 4.2.7 Item limits, query budgets, and errors

The shared item-size expression must supply the exact `item_bytes` rule from section 4.2.1.
The following paths must use it:

- `PartitionStore.measureItemBytes`.
- The put guard in `PartitionStore.upsertItem`.
- The update guard in `PartitionStore.updateItemSingleShot`.
- The compiled update probe in `shared/expression/runtime.ts`.
- The prepare and single-shot checks in `TransactionParticipant`.

The existing item-too-large error and transaction rejection paths remain in place.
A storage coefficient change must not introduce a new prepare rejection or commit failure.
Early client data checks remain distinct from the exact store-side item check.

The query scan must pass `item_bytes` to its candidate consumer.
The collector charges that value for each evaluated item, including an item that the filter rejects.
It retains the existing cursor, count, projection, and response-budget rules.

This changes the evaluated-byte charge and can change page boundaries.
The cursor continues to represent a key position, not a storage coefficient.

#### 4.2.8 Range boundaries

Keep `planRangeSplit` inside one `transactionSync` snapshot.
Its inputs remain the hash key, this leaf's owned `[start, end)` slice, and the requested child count `N`.
The summary must describe exactly that complete owned slice.

Let `C` be its exact item count. Let `B` be its estimated storage bytes under the active model.
For boundary `i`, the cumulative byte target is `i * B / N`.

The planner must:

1. Return `fewer_items` when `C < N`.
2. Read the ordered rows through a streaming covering-index cursor.
3. Calculate each row's weight with the model in section 4.2.4.
4. Measure each candidate prefix at the actual boundary between adjacent keys.
5. Select the feasible prefix nearest the cumulative byte target.
6. Keep at least one item after the previous boundary and one item for each remaining child.
7. Create each boundary with `KeyCodec.shortestSeparator` on the adjacent keys.
8. Return exactly `N - 1` strictly increasing boundaries when `C >= N`.

For a cut after `k` items, the lower prefix contains only those `k` items.
The next item belongs to the upper child and must not contribute to that prefix.

A feasible cut must follow the previous cut and leave enough items for all remaining children.
A dominant item can force a target away from equal balance. The planner must still select a feasible cut.
Equal-distance choices remain open in section 4.3.2.

The scan must remain streaming. It can stop once it has enough information for the last boundary.
It must not use repeated OFFSET walks or materialize the complete slice.

Remove `skewed_bytes` as a normal range floor.
Update `RangeSplitPlan`, `PartitionDO.splitDecision`, and tests that use this floor.
`computeRangeSplitBoundaries` remains the wrapper that returns boundaries or `null` for the item-count floor.

The planner must preserve canonical byte order, strict boundary order, and the slice bounds.
An accounting or ownership inconsistency is not evidence that valid items cannot be split.

#### 4.2.9 Extension for supporting tables

A future table must add facts that describe its actual storage shape.

- A fixed number of rows per item can change the fixed overhead.
- Repeated keys can change the key coefficients.
- Variable row counts need exact counts.
- Variable encoded data needs exact byte totals.
- Conditional rows need aggregates for the items that hold them.

The feature must update its facts with its rows in the same transaction.
Its migration and deletion paths must move or remove those facts with the related item.

New facts can need a schema migration and a bounded backfill.
Adjustments to coefficients for existing facts must not need an item backfill.
Do not create placeholder tables, a cost registry, or a generic plug-in framework for unspecified features.

Temporary transaction rows can need a separate model later.
A larger fixed overhead per committed item does not accurately represent that storage.

#### 4.2.10 Deployment, migration, and rollback

This is a pre-release schema and behaviour change.
The item limit drops the adjustable storage overhead. Query evaluated bytes also drop that overhead.
Range boundaries and promotion decisions use the revised estimate.
Key encoding, DO names, and committed item contents remain unchanged.

The current code can reject an item that the revised exact-item rule accepts.
Therefore, rollback to the old validity rule is not automatically safe.
It can also conflict with a transaction that the new code already prepared.

Existing state must not silently mix old estimates with new facts.
Editing a completed schema migration does not update objects that already ran it.
The policy for existing local objects remains open in section 4.3.4.

If migration preserves existing state, it must preserve items, item IDs, locks, and prepared transactions.
It must prevent size consumers from reading incomplete aggregates.
Any backfill must be bounded and must resume safely after a crash.
A deployment must not delete existing state without explicit approval.

A future coefficient-only change uses the activation rule in section 4.2.5.
It must preserve item validity and transaction commit eligibility.

#### 4.2.11 Verification

Tests must check definitions, not only agreement between two uses of the same estimate.

**Exact facts and aggregate updates**

- Check multibyte text, binary data, JSONB, binary key tags, and an absent sort key.
- Check inserts, overwrites, compiled updates, deletes, and absent deletes.
- Check null-to-TTL, TTL-to-null, and non-null TTL changes.
- Check TTL expiry and per-key aggregate subtraction.
- Check migration retries, page grouping, restart, and source copy cleanup.
- Compare every active aggregate with the corresponding SQL sum and count.
- Check that transactional prepare and cancel do not add committed-item facts.

**Storage models and size consumers**

- Check that row weights sum to the per-key estimate under the same model.
- Change coefficients without touching items and check that all existing rows use the revised estimate.
- Check that model changes leave exact facts, item validity, and query evaluated bytes unchanged.
- Check that a prepared transaction remains eligible to commit after a coefficient change.
- Check the item limit immediately below, at, and above `MAX_ITEM_BYTES`.
- Check that query response estimates remain separate from evaluated-byte charges.
- Check coefficient changes and summary-index activation across restart or interrupted migration.

**Range planner**

- Four equal weights and two children must split into two items per child.
- Three weights `[111, 5111, 111]` and three children must produce three non-empty children.
- Check dominant items at the beginning, middle, and end.
- Check exactly `N` items, fewer than `N` items, an empty slice, and variable sort-key lengths.
- Check non-null TTL costs and variable data sizes.
- Check explicit owned slice bounds and shortest separators.
- Check string and binary keys in canonical byte order.
- Check that children cover the slice without gaps or overlaps.
- Replace the test that expects the dominant-item case to return `null`.

**Query plans and production flows**

- Use `EXPLAIN QUERY PLAN` to assert covering scans for sizes and count queries.
- Check the covering old-facts read on write paths.
- Check the indexed promotion threshold lookup under the active expression.
- Check range split, migration, promotion, and transaction flows in the Workers runtime.
- Use the existing partition harness for repartition tests. Add no production test hook.

Use the repository verification commands in `AGENTS.md` for the code change.
This RFC does not change those commands or the test infrastructure.

### 4.3 Open questions

#### 4.3.1 Initial fixed overheads

TODO: Measure and select `base_overhead` and `ttl_overhead` for the revised table and indexes.
The current 108-byte constant does not establish both values.
Their values affect balance and promotion estimates, but not exact item validity.

#### 4.3.2 Equal-distance boundary choices

TODO: Select a deterministic rule when two feasible prefixes have the same distance from a byte target.
The choices are the earlier or later feasible prefix.
The rule changes boundary placement, not ownership correctness or the non-empty-child requirement.

#### 4.3.3 Summary index and activation

TODO: Select the indexed representation of the derived per-key estimate.
An expression index avoids a persisted price per item. A cached derived summary needs explicit refresh and activation.

TODO: Specify index replacement, restart recovery, and activation for the selected representation.
The procedure must preserve the rules in section 4.2.5.

#### 4.3.4 Existing local state and rollback

TODO: Decide whether the pre-release change must preserve existing local objects through a schema migration.
A fresh-namespace policy is another option, but it cannot silently delete existing data.

TODO: Specify the selected migration steps and a safe rollback policy.
A rollback cannot assume that items or prepared transactions still fit the old validity rule.

## 5. Alternative options

### 5.1 Increase the fixed constant

A larger constant can reduce some underestimation.
It cannot represent variable key copies, separate item validity, or correct the range planner.
Old rows also retain their previous price.

### 5.2 Store one adjustable storage price per item

This makes a scan read a ready-to-use weight.
However, coefficient changes need an item backfill or leave mixed model versions.
Exact facts avoid this dependency.

### 5.3 Add a model version without a recalculation procedure

A version can identify an old price. It does not convert that price to the current model.
It also does not provide indexed totals for a mixture of versions.

### 5.4 Use a generated item-size column

This can reduce explicit write expressions.
The current Workers query-plan tests show that this change loses the needed covering-index property.
Keep an ordinary exact-size column and shared writer expressions.

### 5.5 Use RPC estimates for storage weights

RPC estimators measure a different representation.
Public JSON reads use decoded text, while stored JSON uses JSONB.
The envelope estimate also does not represent SQLite indexes.

### 5.6 Materialize all rows or maintain an order-statistics structure

Whole-slice materialization replaces the streaming memory constraint.
A maintained structure adds work to every mutation.
Neither is needed to correct prefix accounting or preserve non-empty children.

## 6. Frequently asked questions

### Does the new storage estimate equal `sql.databaseSize`?

No. It estimates item-related storage from exact facts.
SQLite pages, shared partition state, and temporary transaction storage remain outside that equality.
The actual database size continues to control the physical cap.

### Does a coefficient change need a schema migration?

It needs no rewrite of item facts.
It can need a summary-index change. Section 4.3.3 leaves that activation procedure open.
A new fact for a supporting table can need a bounded backfill.

### Can a split remain unequal?

Yes. An item cannot cross a boundary.
A dominant item can make equal child sizes impossible.
With enough items, the planner still produces the requested number of non-empty children.

### Can a child still exceed the cap after a valid split?

Yes. A single item's physical footprint can exceed a configured cap.
A valid split does not promise that every child fits that cap.
It separates the available items instead of declaring byte skew an impossible split.

### Why use JSONB bytes for exact item size?

This preserves the existing stored-representation measurement.
It avoids a separate JSON text measurement for item validity.
The rule remains distinct from the RPC response estimate and exact DynamoDB compatibility.

### Can future storage-model changes be compatible?

Yes. Separate item validity from storage prices and keep exact reusable facts.
Changes to existing coefficients then affect storage decisions without changes to accepted item contents.
New facts still need an explicit schema and migration design.

## 7. References

Code paths below are relative to `packages/fokosdb/src`:

- `shared/partition/item-size.ts` — the current stored-size formula and data expression.
- `shared/partition/partition-store.ts` — schema, size facts, mutations, query scans, and range boundaries.
- `shared/partition/partition-store.test.ts` — size, aggregate, covering-index, and boundary tests.
- `shared/partition/fokos-migration-host.ts` — per-page migration accounting.
- `shared/partition/transaction-participant.ts` — prepare checks and committed mutations.
- `shared/expression/runtime.ts` — compiled update-size probes.
- `shared/query/query-collector.ts` — evaluated-byte and response-byte charges.
- `server/do-partition.ts` — admission, promotion checks, split decisions, and host integration.

Repository documents:

- `AGENTS.md` — invariants, test harnesses, and verification commands.
- `docs/agent-plans/range-split-boundary-byte-seek.md` — streaming byte-based boundary scan.
- `docs/agent-plans/2026-09-27-over-size-split-trigger.md` — physical cap, promotion decisions, and range floors.

Platform references:

- [SQLite file format: SQL indexes](https://www.sqlite.org/fileformat2.html#representation_of_sql_indices).
- [Durable Object SQLite storage API](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/).
- [Durable Object limits](https://developers.cloudflare.com/durable-objects/platform/limits/).
