# FokosDB

FokosDB is a globally strongly-consistent key-value database on Cloudflare Durable Objects. Its API and transaction model follow DynamoDB. It ships as the `fokosdb` npm package.

## Rules

- Write in Simplified Technical English (ASD-STE100). `.claude/skills/spec-write/references/ste-rules.md` has the rules.
- Correctness and reliability come first. Write as little code as the task needs.
- A code comment must stand alone. Never name a discussion, a report, a plan, or a feature that the codebase does not contain.
- Run `pnpm test` in a subagent. Its output is long.
- Your knowledge of the Workers platform can be out of date. Read the current [Workers](https://developers.cloudflare.com/workers/) and [Durable Objects](https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/) documentation before you change either, and read a limit from the product's `/platform/limits/` page.
- Do not add a production hook for a test.

## Commands

This is a pnpm workspace. Run the scripts of the root `package.json` from the repository root.

- `pnpm build`, `pnpm test`, `pnpm check` (build, lint, typecheck, format), `pnpm fmt`.
- `pnpm cf-typegen` after you change a binding. Each wrangler project keeps its own `worker-configuration.d.ts` and its own `.wrangler/` state.
- The examples import the built `dist/`, so a source change needs a build. `pnpm test` and `pnpm dev` build first.
- There are two wrangler projects: `packages/fokosdb/wrangler.jsonc` gives vitest an entrypoint and is never deployed, and `examples/http-api/wrangler.jsonc` is the deployable example.
- `.github/workflows/preview-release.yml` publishes a preview build through pkg.pr.new. Keep it to ONE `pkg-pr-new publish` call and pass extra packages as extra arguments, because a second call counts as spam.

## Package layout

`packages/fokosdb/src` has three parts. Convention keeps them apart, not the module system.

- `client/` — `db.ts` and the entry barrel. Published as `fokosdb/client`.
- `server/` — the two Durable Object classes. Published as `fokosdb/server`.
- `shared/` — what both sides use. tsdown inlines it into whichever entry reaches it.

**The client must never import a Durable Object class as a value.** That pulls the whole server implementation into `dist/client`. Use the type-only helpers in `shared/do-stubs.ts` and keep every class import `import type`. `pnpm build` enforces the rule, pins the packages the client may import, and holds the client bundle under a size budget.

A cohesive folder stays whole inside `shared/` even when only one side uses it. An entry pulls in only the modules it names.

## Architecture

- **`PartitionDO`** (`src/server/do-partition.ts`) — holds items in SQLite, one DO per partition shard. It serves single-item reads and writes, acts as a resource manager in 2PC, and splits itself when it grows past its cap.
- **`TransactionCoordinatorDO`** (`src/server/do-transaction-coordinator.ts`) — one DO per write transaction, named by the idempotency token. It drives 2PC. A read transaction runs in the Worker instead.
- **`FokosDB`** (`src/client/db.ts`) — the client entry point. It routes with `PartitionTopologyRouterImpl`, sends a multi-partition write to a coordinator, and drives a multi-partition read itself.

An item has a `hashKey`, an optional `sortKey` (default `""`), data as `Uint8Array | string`, a `version` that every write increments, and an optional TTL.

**`PartitionContext` travels in every RPC.** Workers RPC cannot configure a DO at instantiation, so the topology configuration goes with each request and the DO compares it with the one it stored. Never read `env[ctx.ns]` outside `shared/do-stubs.ts`: use `partitionNamespace`, `txCoordinatorNamespace`, or the stub helpers, because each one applies the configured jurisdiction.

## Partitions

- `rootTreesN` root partitions exist at startup, and a hash of the hash key selects one. A partition ID is opaque: read it only through `PartitionIdHelper`.
- **Hash split** — a partition past `hashSplitConditions.maxSizeMb` queues a split, creates `hashSplitN` children, becomes a forwarding router, and the children import their share in the background. Its states are `split_queued`, `split_started` and `split_completed`.
- **Promotion** — one hash key past `hashSplitConditions.maxSizeMb * RANGE_PROMOTION_FRACTION` moves into a range tree of its own, which then splits by sort key.
- **`splitN` must never change after initialization.** A change breaks routing and loses data.
- A partition refuses a write above 1.1 times its cap, and only a write that applies can queue the split that brings it back under.

## queryItems

`queryItems` returns one bounded page. A caller follows `cursor` until it is absent, and a page can hold no items and still carry a cursor. `select` is `"projection"` or `"count"`. A request can also carry a `filter` and a `projection`; SQLite evaluates both and JavaScript evaluates neither.

Four budgets bound one page: evaluated items (`limit`), evaluated bytes, response bytes (`maxResponseBytes`), and partition visits. `QueryPageBudget` (`shared/query/page-budget.ts`) carries them across the sub-queries in `FokosDB` and across the children in `walkRangeChildren`.

## Transactions (2PC)

The model follows the DynamoDB papers: [ATC 2023, Idziorek et al.](https://www.usenix.org/system/files/atc23-idziorek.pdf) and [ATC 2022, Elhemali et al.](https://www.usenix.org/system/files/atc22-elhemali.pdf)

- Coordinator states are `CREATED → PREPARING → PREPARED → COMMITTING → COMMITTED`, or `→ CANCELLING → CANCELLED`. Every transition writes to SQLite BEFORE it sends an RPC.
- **`PREPARED` is the point of no return.** A prepared transaction must commit, and the coordinator never goes from `PREPARED` to `CANCELLING`.
- `prepare`, `commit` and `cancel` are idempotent. The `items` table holds committed state only, and `pending_transactions` holds the locks of the in-flight transactions.
- A non-transactional write to a locked item is REFUSED, not delayed.
- A read transaction reads twice and compares `found`, `version` and the partition's `deleteRevision`. Any change aborts it with `read_conflict`.
- `clientRequestToken` names the coordinator DO and gives idempotency. A retry must use the same coordinator pool size.

## Rules for PartitionDO operations

Every write or transaction RPC meets two concurrent state machines: **migration** (a child that still imports) and **split** (a parent that now routes). A mistake here loses data or leaks a lock forever.

- **Migration guard** — call `await this.ensureMigration("<op>")` near the top of every write and transaction RPC, after `ensurePartitionContext`. A read that tolerates stale data uses `ensureMigration("<op>", false)`, which reads through to the parent. Never guard the migration RPCs themselves (`getItemsBatch`, `getPartitionTransactionMetadata`, `acknowledgeChildMigrationComplete`), because they are what moves the migration forward.
- **Split routing** — `putItem`, `deleteItem` and `getItem` use `withSplitForwarding`. `prepare`, `commit` and `readForTransaction` use `groupItemsByRouting` and then fan out. `cancel` must reach the children at `split_started` AND at `split_completed`, or their pending rows stay forever.
- **Never swallow a child error** — try every child, collect the failures, then rethrow, so the coordinator stays non-terminal and retries until every child answers.
- **Background recovery** — a split parent and an importing child must skip stale-transaction recovery; use the `txPendingCanSweep` guard. Apply a terminal outcome through the PUBLIC `commit()` and `cancel()`, never through inline SQL or a private helper, because only the public methods hold the migration guard and the split routing.

## Testing

Tests run in the real Workers runtime through `@cloudflare/vitest-pool-workers`. Each suite makes its own namespace with a `crypto.randomUUID()` prefix.

- `test/partition-do/` holds one file per `PartitionDO` behaviour, `test/transactions/` the transaction suites, and `test/repartition/` the repartition flows.
- Use `makeStub` (`test/partition-do/helpers.ts`) for an ordinary test. Use `TestPartition` (`partition-harness.ts`) only when the test drives a split, a migration, or a promotion, with `triggerHashSplit`, `triggerRangeSplit`, `splitHash`, `splitRange`, `makeRangeRoot`, `runAlarm` and `drainUntil`.
- Property-based suites are in `test/property-based/` and use `fast-check`. `arbitraries.ts` holds the shared arbitraries, `model.ts` the stateful model and its commands, and `query-model.ts` the key oracle of the query suites. Every arbitrary produces input that the public API accepts, so a failure is a library bug.
- A suite whose fixture is expensive builds it once in `beforeAll` and shares it across the runs. `transactions-split.test.ts` gives each run its own key prefix because its runs write; `query-items-split.test.ts` needs neither, because a query changes nothing.
- Give every property `it` a large explicit timeout, because shrinking reruns the scenario many times. `propertyRuns(default)` sets the run count, `FOKOS_PROPERTY_RUNS=500 pnpm vitest run test/property-based/` searches deeper, and a failure replays from the printed `seed`, `path` and `replayPath`.
- Global fake timers can run a DO background callback in the wrong I/O context. Lifecycle tests use real timers and the scheduled-alarm test APIs instead.

## Where the detail lives

- `docs/adr/` — architecture decisions.
- `docs/agent-plans/` — one dated specification per feature. Read the matching one before you change that feature.
- `docs/ideas/` — proposals that are not decided yet.
