# FokosDB

FokosDB is a globally strongly-consistent key-value database built on Cloudflare Durable Objects, inspired by DynamoDB's API and transaction model. It is a library published as the `fokosdb` npm package.

## Critical tips

- Always run tests `pnpm test` in a subagent to not pollute the context with the verbose output.
- Your knowledge of Cloudflare Workers APIs and limits may be outdated. Always retrieve current documentation before any [Workers](https://developers.cloudflare.com/workers/) and [Durable Objects](https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/) tasks. For all limits and quotas, retrieve from the product's `/platform/limits/` page. eg. `/workers/platform/limits`.

## Commands

This is a pnpm workspace. Run these from the repo root.

| Command           | Purpose                                              |
| ----------------- | ---------------------------------------------------- |
| `pnpm build`      | Build the library into `packages/fokosdb/dist`       |
| `pnpm test`       | Build, then run the library and example suites       |
| `pnpm dev`        | Build, then run the example workers locally          |
| `pnpm check`      | Typecheck every package and check formatting         |
| `pnpm lint:pkg`   | `publint` on the packaged library                    |
| `pnpm cf-typegen` | Generate TypeScript types for every wrangler project |

The examples import the library's built `dist/`, not its sources, so a source change needs a `pnpm build` before an example picks it up. `pnpm test` and `pnpm dev` already do this.

`.github/workflows/preview-release.yml` publishes an installable preview build of the library for
every commit on `main`, and for pull requests opened from a branch of this repository, through [pkg.pr.new](https://pkg.pr.new/). It runs `pnpm build`
first, so the client-bundle guards gate every published build. Keep the `pkg-pr-new publish` call
to one invocation in that workflow, and pass extra packages as extra arguments; a second invocation
is treated as spam. The workflow publishes only when `lambrospetrou` triggers it, and the username
is written out in the workflow, so it needs an edit if the account or the repository owner changes.

There are two wrangler projects:

- `packages/fokosdb/wrangler.jsonc` — the library worker. It is never deployed. It gives `vitest` and `wrangler types` an entrypoint (`packages/fokosdb/test/worker-entry.ts`) that exports the library Durable Objects.
- `examples/http-api/wrangler.jsonc` — the deployable example HTTP API worker, with its own `public/` assets, secrets and generated types.

Run `pnpm cf-typegen` after changing bindings in either file. Each project has its own `worker-configuration.d.ts` and its own local state under `.wrangler/`.

## Package layout

`packages/fokosdb/src` splits three ways, and the split is enforced by convention, not by the module system:

- `client/` — `db.ts` plus the entry barrel. Published as `fokosdb/client`.
- `server/` — the two Durable Object classes plus the entry barrel. Published as `fokosdb/server`.
- `shared/` — everything both sides use: key codec, expression engine, partition topology, partition store, transaction types. Not published on its own; tsdown inlines it into whichever entry reaches it.

**The client must never import a Durable Object class as a value.** Doing so pulls the whole server implementation into `dist/client`. Use the type-only helpers in `shared/do-stubs.ts` to get a typed stub, and keep class imports on the client side `import type`. `shared/partition-errors.ts` exists for the same reason: the client matches on those errors, and the predicates must not drag in `do-partition.ts`.

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

## Transaction Protocol (2PC)

Modeled after the [_"Distributed Transactions at Scale in Amazon DynamoDB"_ USENIX ATC 2023 paper (Idziorek et al.)](https://www.usenix.org/system/files/atc23-idziorek.pdf) and the [_Amazon DynamoDB: A Scalable, Predictably Performant, and Fully Managed NoSQL Database Service_ USENIX ATC 2022 paper (Elhemali et al.)](https://www.usenix.org/system/files/atc22-elhemali.pdf).

**Write transactions (`transactWriteItems`)**:

- TC state machine: `CREATED → PREPARING → PREPARED → COMMITTING → COMMITTED` (or `→ CANCELLING → CANCELLED`)
- Every state transition writes to SQLite **before** sending outbound RPCs (write-ahead).
- `PREPARED` is the point of no return — a PREPARED transaction MUST eventually commit.
- Conflict detection: `last_transaction_ts` column on items; `max_deleted_ts` in `deletion_metadata` for items that were deleted.
- Non-transactional writes (`putItem`/`deleteItem`) are **rejected** (not delayed) if a pending transaction holds the item's lock.
- TC recovery: PartitionDO alarms poke stale TCs via `recoverTransaction()`; TC alarm retries stale in-flight transactions.
- TC storage: payload is stripped at `PREPARED` or `CANCELLING`; item and participant rows are deleted only at the terminal transition.
- Idempotency: `clientRequestToken` is 1 to 64 UTF-8 bytes and names the TC DO. The terminal `tc_state` row remains for 10 minutes after completion, then the TC alarm deletes it.

**Read transactions (`transactGetItems`)**:

- Two-phase double-read: read once, check no pending writes, read again, compare `lastCommittedTs`. If anything changed → abort.
- The Worker drives both phases directly. No coordinator or durable read state exists. If the Worker stops mid-read, the client retries.

**Key invariants**:

1. `items` table always contains committed state only.
2. `pending_transactions` holds locks for in-flight transactions.
3. `prepare`, `commit`, `cancel` are all idempotent.
4. TC never transitions from PREPARED to CANCELLING.

## Testing

Tests run in the actual Cloudflare Workers runtime via `@cloudflare/vitest-pool-workers`. Each test suite creates isolated namespaces using `crypto.randomUUID()` prefixes. Integration tests are in `packages/fokosdb/test/transactions.test.ts`; partition/topology tests in `packages/fokosdb/src/server/do-partition.test.ts`.

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
