# FokosDB

FokosDB is a globally strongly-consistent key-value database built on Cloudflare Durable Objects, inspired by DynamoDB's API and transaction model. It is a library published as the `fokosdb` npm package.

## Critical tips

- Use Simplified Technical English (ASD-STE100) language as defined in `.claude/skills/spec-write/references/ste-rules.md`.
- Correctness and reliability above everything, with as little code as necessary to achieve what we need.
- When you write comments inline the code do not refer to discussion references like W1 or W2 or report XYZ. Those do not mean anything to future readers. Your comments should always be stand-alone and not refering to ideas or bainstorming discussions and features that never shipped. Never reference anything not in the current codebase.
- Always run tests `pnpm test` in a subagent to not pollute the context with the verbose output.
- Your knowledge of Cloudflare Workers APIs and limits may be outdated. Always retrieve current documentation before any [Workers](https://developers.cloudflare.com/workers/) and [Durable Objects](https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/) tasks. For all limits and quotas, retrieve from the product's `/platform/limits/` page. eg. `/workers/platform/limits`.

## Commands

This is a pnpm workspace. The `scripts` in the root `package.json` are the entry points. Run them from the repo root.

The examples import the library's built `dist/`, not its sources, so a source change needs a `pnpm build` before an example picks it up. `pnpm test` and `pnpm dev` already do this.

`.github/workflows/preview-release.yml` publishes an installable preview build of the library for
every commit on `main`, and for pull requests opened from a branch of this repository, through [pkg.pr.new](https://pkg.pr.new/). It runs `pnpm build`
first, so the client-bundle guards gate every published build. Keep the `pkg-pr-new publish` call
to one invocation in that workflow, and pass extra packages as extra arguments; a second invocation
is treated as spam. The workflow publishes only when `lambrospetrou` triggers it, and the username
is written out in the workflow, so it needs an edit if the account or the repository owner changes.
Preview install URLs are documented as `pkg.pr.new/lambrospetrou/fokosdb/fokosdb@<sha>`.

There are two wrangler projects:

- `packages/fokosdb/wrangler.jsonc` — the library worker. It is never deployed. It gives `vitest` and `wrangler types` an entrypoint (`packages/fokosdb/test/worker-entry.ts`) that exports the library Durable Objects.
- `examples/http-api/wrangler.jsonc` — the deployable example HTTP API worker, with its own `public/` assets, secrets and generated types.

Run `pnpm cf-typegen` after changing bindings in either file. Each project has its own `worker-configuration.d.ts` and its own local state under `.wrangler/`.

## Package layout

`packages/fokosdb/src` splits three ways, and the split is enforced by convention, not by the module system:

- `client/` — `db.ts` plus the entry barrel. Published as `fokosdb/client`.
- `server/` — the two Durable Object classes plus the entry barrel. Published as `fokosdb/server`.
- `shared/` — everything both sides use: key codec, expression engine, partition topology, partition store, transaction types. Not published on its own; tsdown inlines it into whichever entry reaches it.

**The client must never import a Durable Object class as a value.** Doing so pulls the whole server implementation into `dist/client`. Use the type-only helpers in `shared/do-stubs.ts` to get a typed stub, and keep class imports on the client side `import type`. The error classes live in `shared/errors.ts` and `shared/errors-operations.ts` for the same reason: the client raises and matches on them, and a match must not drag in `do-partition.ts`.

`pnpm build` enforces this rule. The `check-client-bundle` plugin in `packages/fokosdb/tsdown.config.ts` walks the chunks that the client entry imports and fails the build if a module below `src/server/` is in one of them. The same plugin pins the external packages that the client may import and holds the client bundle under a size budget. It also prints the raw, minified and gzipped size of each entry together with the chunks that the entry imports, which is what a consumer really ships; `esbuild` is a devDependency for that minify step. Keep the plugin inline in the `plugins` array: `defineConfig` gives its hooks their types, so a separate helper would need `rolldown` as a devDependency only for the plugin types.

Cohesive folders stay whole inside `shared/` even when only one side uses them. An entry pulls in only the modules it names, so placing a server-only module in `shared/` costs the client bundle nothing.

## Architecture

Two Durable Object classes do all the work:

- **`PartitionDO`** (`packages/fokosdb/src/server/do-partition.ts`) — stores items in SQLite. One DO per partition shard. Handles single-item reads/writes and participates in 2PC as a transaction resource manager. Automatically splits into child partitions when storage thresholds are met.
- **`TransactionCoordinatorDO`** (`packages/fokosdb/src/server/do-transaction-coordinator.ts`) — one DO per write transaction (named by idempotency token). Drives 2-phase commit across multiple PartitionDOs. Read transactions run in the Worker.

The `FokosDB` class (`packages/fokosdb/src/client/db.ts`) is the client-side entry point. It routes requests with `PartitionTopologyRouterImpl`, delegates multi-partition writes to `TransactionCoordinatorDO`, and drives multi-partition reads directly.

The coordinator pool uses the shard group `fokos_tc.<tableName>`. Its size is `numTxCoordinators` or, by default, two shards per root partition. Retries with the same `clientRequestToken` must use the same pool size. In-flight recovery uses the coordinator ID in each participant lock and does not depend on the current pool size.

### Data Model

Items are keyed by `hashKey` (required) + `sortKey` (optional, defaults to `""`). Data is `Uint8Array | string`. Items have a `version` counter (incremented on every write) and an optional TTL.

## Partition Topology & Routing

- At startup, `rootTreesN` root partitions are created (e.g. 10).
- Routing uses hashing to map `hashKey` to a root partition index.
- Partition IDs are opaque hex-encoded bytes and encodes the data partition location in the entire partitions topology. The opaque partition ID should only be accessed through the `PartitionIdHelper` class.
- **`PartitionContext` is passed in every RPC call** — DOs cannot be configured at instantiation time in Workers RPC, so the topology config (splitN, ns, tableName, etc.) travels with every request. The DO validates the context matches its stored one.
- The `PartitionTopologyRouterImpl` is used by the client (`FokosDB`) to pick partitions. `PartitionTopologyImpl` is used inside the DOs for split management.

## Partition Splitting

When a PartitionDO's SQLite size exceeds `hashSplitConditions.maxSizeMb`, it queues a hash split:

1. **`split_queued`**: After a write, `maybeQueueSplit` detects the threshold and queues. An alarm fires.
2. **`split_started`**: `startSplit` initializes `N` child DOs via `initFromSplit`. The parent becomes a forwarding proxy. Children begin migrating data in background.
3. Child migration: children call `getItemsBatch` + `getPartitionTransactionMetadata` on the parent via paginated RPC batches (~20 MB per batch). The parent filters only rows belonging to that child using the same hash function.
4. **`split_completed`**: Once all children acknowledge migration complete, the parent transitions. Reads during migration go directly to parent (`getItemDirect`). Writes are rejected with a 503 during migration.

**Critical**: `splitN` must NOT change after initialization — it would break routing and cause data loss.

## queryItems paging

`queryItems` returns one bounded page per call. `select` is `"projection"` (materialized items, the default) or `"count"` (`items: []` and the matched count of the page). The `select` value names the selection mode and is not the `projection` expression of the next subsection: a `"projection"` page materializes complete items, or projected records when the request carries a `projection`. A caller follows `cursor` until it is absent; a page can hold zero items and still carry a cursor.

Four budgets bound a page, and `shared/query/page-budget.ts` holds their values: the evaluated-item budget (`limit`, default 1,000, maximum 100,000), a fixed evaluated-byte budget over the stored `est_row_bytes` of the evaluated items, the response-byte budget (`maxResponseBytes`) over the materialized items, and the partition-visit budget. `QueryPageBudget` tracks them across sub-queries in `FokosDB` and across children in `walkRangeChildren`; both pass the remaining values and `allowOversizedFirstItem` down in every RPC. The first materialized item of a page can exceed the response budget once, for the whole page and not once per leaf.

The leaf scan is `PartitionStore.scanQueryPage` plus `shared/query/query-collector.ts`. Count mode reads only `sk` and `est_row_bytes` from the covering `idx_items_scan` index. The statement binds `LIMIT remainingEvaluatedItems + 1`: the extra row tells a stopped page from a drained interval. A candidate that a budget rejects stops the page with an inclusive `nextCursor` at that candidate and is not counted; a range router that exhausts a budget resumes exclusively after `lastEvaluatedCursor`. `meta.rowsRead` and `meta.rowsReturned` are physical SQLite metrics and are never derived from `count` or `scannedCount`. Migration keeps `collectBatch` and its own byte budget; do not merge the two collectors.

### Read projections and query filters

`queryItems` accepts `filter?: ConditionExpression` and `projection?: readonly ProjectionExpression[]`. `getItem` and each item of `transactGetItems` accept `projection`. `FokosDB` validates and compiles the expressions before it routes the request: `compileQueryExpression` builds one `CompiledQueryPlan` for both halves of a query, so a path or a literal that both halves use binds once, and `compileProjectionExpression` builds the `CompiledProjectionPlan` of a point read. `docs/agent-plans/2026-09-14-read-projections-and-query-filters.md` is the specification.

- SQLite evaluates both, and JavaScript evaluates neither. The filter is a `matched` result column and never a `WHERE` term, so a rejected candidate still reaches `collectQueryPage`: it consumes both evaluated budgets, advances the cursor, adds zero response bytes, and stays out of `items`. `count` is the matched count of the page and `count <= scannedCount`; the two are equal only for a request with no filter. A filter never changes candidate routing or the sort-key interval.
- A projected point read (`getItem`, each `transactGetItems` item) returns its flat record as `data` in the ordinary item envelope, with `kind: "projected"` and the usual `version` and `ttlAt`. `ReadItem<T>` in `shared/types.ts` is that envelope and every read returns it: a caller reaches the value through `item.data` and narrows on `kind`, never on the presence of a field, and no read method needs a projected overload. `"projected"` is a public read-result kind only — do not add it to `DataKind`, whose index is the on-disk `data_kind` code. A projected `queryItems` page keeps bare records, and is the one overload that remains, because a page names its projection once and its overload types every element exactly. `QueryItemsPage<Item>` declares that page around its element, because the element is all that differs between the two forms, and both `QueryItemsResult<T>` and `QueryItemsProjectedResult<T>` are aliases of it. Keep `T` meaning the caller's own data type on every read method: a page parameterized by its element instead would make `QueryItemsResult<MyType>` compile as the projected shape.
- The three read methods take an optional type parameter. It types `data` for `kind: "json"` and for `kind: "projected"` and nothing else. `getItem<T>` and `queryItems<T>` take one type. `transactGetItems<Ts>` takes a tuple, one member per item by position, because one request reads unrelated items; the tuple also fixes the item count, and an array type such as `Profile[]` gives one type to every position. Its options are wrapped in `NoInfer`, which keeps `Ts` off the inference path — the request keys would otherwise infer it as `unknown` per position and destroy the default of a call that names no type. `packages/fokosdb/src/client/db.test.ts` holds that contract as `@ts-expect-error` cases. The store holds opaque data, so the library never checks `T`; `db.ts` casts to it at the public method and keeps every private method on the default instantiation. `CallerType` resolves the `never` default back to the widest type the library can return, so a call without `T` keeps the types it had.
- The wire row is positional (`ProjectedWireRow`, one value column and one type column per entry) and carries no names: the client owns the compiled plan, and `projectedItemFromWireRow` builds the record at the public boundary. The RPC variants keep the row under `projected` beside `kind`, `version`, and `ttlAt`.
- Both plans use the `"pool"` binding layout: every literal and every path is one element of one JSON array bound as `?1`, so the SQL parameter count does not grow with the expression. A query statement numbers its scan parameters from `?2`; a projected point read binds `hk` as `?2` and `sk` as `?3`. The pool is bound always, as the text `"[]"` when the plan has no descriptor.
- The cursor fingerprint covers `filterIdentity` and `projectionIdentity`, so a cursor is rejected with `cursor_fingerprint_mismatch` when either changes. A request with neither appends nothing, so a cursor issued before this feature stays valid.
- `select: "count"` with a `projection` is rejected with `query_projection_with_count`. With a `filter` it is valid.
- `transactGetItems` rejects two items that name one key with `transact_duplicate_key`: the two-phase driver pairs the phases by key, and per-item projections make a positional answer ambiguous.
- Routing carries the plans and needs no code of its own: `withSplitForwarding`, `walkRangeChildren`, the migration fallback, and the transaction fan-out all spread the request they received.

## Transaction Protocol (2PC)

Modeled after the [_"Distributed Transactions at Scale in Amazon DynamoDB"_ USENIX ATC 2023 paper (Idziorek et al.)](https://www.usenix.org/system/files/atc23-idziorek.pdf) and the [_Amazon DynamoDB: A Scalable, Predictably Performant, and Fully Managed NoSQL Database Service_ USENIX ATC 2022 paper (Elhemali et al.)](https://www.usenix.org/system/files/atc22-elhemali.pdf).

**Write transactions (`transactWriteItems`)**:

- TC state machine: `CREATED → PREPARING → PREPARED → COMMITTING → COMMITTED` (or `→ CANCELLING → CANCELLED`)
- Every state transition writes to SQLite **before** sending outbound RPCs (write-ahead).
- `PREPARED` is the point of no return — a PREPARED transaction MUST eventually commit.
- Conflict detection: `last_read_ts` and `last_write_ts` columns on items (a `check` compares with `last_write_ts` and advances only `last_read_ts`; a put, update, or delete compares with `last_read_ts` and advances both); `max_delete_tx_order_ts` in `deletion_metadata` for items that were deleted. Every transaction order timestamp is `Date.now() * TX_ORDER_TS_UNITS_PER_MS` from `txOrderTimestampNow()`.
- Non-transactional writes (`putItem`/`deleteItem`) are **rejected** (not delayed) if a pending transaction holds the item's lock.
- TC recovery: PartitionDO alarms poke stale TCs via `recoverTransaction()`; TC alarm retries stale in-flight transactions.
- TC storage: payload is stripped at `PREPARED` or `CANCELLING`; item and participant rows are deleted only at the terminal transition.
- Idempotency: `clientRequestToken` is 1 to 64 UTF-8 bytes and names the TC DO. The terminal `tc_state` row remains for 10 minutes after completion, then the TC alarm deletes it.

**Read transactions (`transactGetItems`)**:

- Two-phase double-read: read once, check no pending content mutation (a pending `check` does not count), read again, compare `found`, `version`, and the partition's `deleteRevision` (a counter that every user delete of a row advances; the TTL sweep does not). If anything changed → abort with `read_conflict`. An unrelated user delete in the same partition is a conservative `read_conflict`.
- The Worker drives both phases directly. No coordinator or durable read state exists. If the Worker stops mid-read, the client retries.

**Key invariants**:

1. `items` table always contains committed state only.
2. `pending_transactions` holds locks for in-flight transactions.
3. `prepare`, `commit`, `cancel` are all idempotent.
4. TC never transitions from PREPARED to CANCELLING.

## Testing

Tests run in the actual Cloudflare Workers runtime via `@cloudflare/vitest-pool-workers`. Each test suite creates isolated namespaces using `crypto.randomUUID()` prefixes. Integration tests are in `packages/fokosdb/test/transactions.test.ts`. The `PartitionDO` suites live in `packages/fokosdb/test/partition-do/`, one file per behaviour. Use the small `makeStub` factory in `helpers.ts` for ordinary tests. Use `TestPartition` from `partition-harness.ts` only when a test drives a split, migration, or promotion.

Use `triggerHashSplit`, `triggerPromotion`, and `triggerRangeSplit` when the test must inspect a transition. Use `splitHash` and `splitRange` when it needs a completed split. Filler hash keys belong to the target partition and are spread across its children. `makeRangeRoot` creates an empty range root without testing promotion detection; `makeTriggeredRangeRoot` also crosses the range-split threshold and returns all fixture sort keys. `runAlarm()` fires one scheduled alarm pass; the `await*` helpers and `drainUntil` poll durable state and run alarms only when progress stalls.

For migration-in-progress tests, install `withMigrationHeld` before the crossing write. Its wait function returns after every child has reached the real transaction-metadata RPC. Cleanup releases the RPCs and restores the method in `finally`. Use `withMigrationBatchCap` to force cursor-paginated multi-batch migration without touching the real byte budget. Do not add production test hooks for tests.

Global fake timers can run Durable Object background callbacks in the wrong I/O context. The transaction suite currently uses `vi.useFakeTimers({ shouldAdvanceTime: true })` and can log cross-object TTL errors even when its assertions pass. Partition lifecycle tests use normal timers and scheduled-alarm test APIs instead.

## Rules for PartitionDO operations

Every write or transaction RPC on `PartitionDO` must account for two concurrent state machines: **migration** (child catching up from parent) and **split** (parent routing to children). Failing to do so causes data loss or permanent lock leaks.

**NOTE**: Once there is a native Durable Objects API to fork/clone/snapshot existing DO storage, we can scrap the entire migration flow (split will still be the same).

### Migration guard

A child partition in `migration_migrating` has not yet received all data or pending locks from its parent. Any operation that reads or writes local state during this window may act on incomplete data.

- **All write and transaction RPCs** (`putItem`, `deleteItem`, `prepare`, `commit`, `cancel`, `readForTransaction`) must call `await this.ensureMigration("<opName>")` near the top, after `ensurePartitionContext`. This throws a 503-style error when the partition is still migrating, causing the caller to retry once migration completes.
- **Read RPCs** that tolerate stale data (e.g. `getItem`) use the `false` variant — `ensureMigration("getItem", false)` — which reads directly from the parent instead of throwing.
- Do **not** add `ensureMigration` to migration-protocol RPCs themselves (`getItemsBatch`, `getPartitionTransactionMetadata`, `acknowledgeChildMigrationComplete`) — these are the mechanism that drives migration forward.

### Split routing

A parent partition in `split_started` or `split_completed` no longer owns any key ranges — children do. Operations that write or lock items must be forwarded to the correct child; operations that act on already-forwarded locks must reach every relevant child.

- **Item writes** (`putItem`, `deleteItem`) and **reads** (`getItem`) use `withSplitForwarding`, which handles routing automatically.
- **Transaction RPCs** (`prepare`, `commit`, `readForTransaction`) call `groupItemsByRouting` to split items between local and forwarded sets, then fan out to the appropriate child stubs.
- **`cancel`** must forward to children at both `split_started` **and** `split_completed`. After the last child acknowledges migration the parent transitions to `split_completed`; a cancel arriving after that transition must still reach children or their pending rows are never cleaned up.
- When forwarding to multiple children, **do not swallow child errors**. Collect failures, attempt every child, then rethrow if any failed — so the TC stays in a non-terminal state and retries until all children are reachable.

### Background recovery (stale-TX alarm)

A split parent in `split_started` or `split_completed` and a child in `migration_initialized` or `migration_migrating` must skip stale-transaction recovery. Use the independent `txPendingCanSweep` guard. These partitions do not own authoritative, complete lock state.

A `not_found` result has three paths. Delete directly when all keys route away. Cancel an owned lock that is no older than `IDEMPOTENCY_WINDOW_MS`. Quarantine an older owned lock by setting `guarded_at`, log the lock-age guard error once, and wait for `debugForceResolveTransaction`. Guarded transactions must stay out of the stale scan and its alarm scheduling.

When the stale-TX alarm calls `recoverTransaction` on the TC and gets a terminal outcome back (`COMMITTED` / `CANCELLED`), it must apply the outcome by calling the **public** `commit()` / `cancel()` methods — not by inlining SQL or calling private helpers. The public methods encode the migration guard and split routing; bypassing them can write data to the wrong partition or skip child forwarding. `debugForceResolveTransaction` follows the same rule.
