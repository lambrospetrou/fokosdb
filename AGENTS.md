# FokosDB

FokosDB is a strongly consistent, distributed database with bottomless storage, built on top of Cloudflare Durable Objects.
The README.md file has more high-level information for the features.

## Rules

- Write in Simplified Technical English (ASD-STE100). `.claude/skills/spec-write/references/ste-rules.md` has the rules.
- Correctness and reliability come first. Write as little code as the task needs.
- A code comment must stand alone. Never name a discussion, a report, a plan, or a feature that the codebase does not contain.
- Run `pnpm test` in a subagent. Its output is long.
- Your knowledge of the Workers platform can be out of date. Read the current [Workers](https://developers.cloudflare.com/workers/) and [Durable Objects](https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/) documentation before you change either, and read a limit from the product's `/platform/limits/` page.
- Do not add a production hook for a test.
- Before you change a feature, read its specification in `docs/agent-plans/`.

## Commands

This is a pnpm workspace. Run the scripts of the root `package.json` from the repository root.

- `pnpm build`, `pnpm test`, `pnpm check` (build, lint, typecheck, format), `pnpm fmt`.
- `pnpm cf-typegen` after you change a binding.
- The examples import the built `dist/`, so a source change needs a build. `pnpm test` and `pnpm dev` build first.
- `packages/fokosdb/wrangler.jsonc` is only the vitest entrypoint. `examples/http-api/wrangler.jsonc` is the deployable example.
- The preview-release workflow must keep ONE `pkg-pr-new publish` call. A second call counts as spam.

## Package layout

`packages/fokosdb/src` has four parts. Convention keeps them apart, not the module system.

- `client/` — the `FokosDB` client. Published as `fokosdb/client`.
- `server/` — the Durable Object classes. Published as `fokosdb/server`.
- `shared/` — what both sides use.
- `sharding/` — the sharding library. It uses no FokosDB module. Published as `fokosdb/sharding/client` and `fokosdb/sharding/server`.

**The client must never import a Durable Object class or a server runtime module as a value.** Keep each such import `import type`. `pnpm build` enforces this rule and a size budget on the client bundle.

## Architecture

- **`FokosDBPartitionDO`** — one DO for each partition. It holds items in SQLite, serves single-item operations, is a participant in 2PC, and splits when it grows past its size cap.
- **`FokosDBTransactionCoordinatorDO`** — drives 2PC for a write transaction. The idempotency token routes to it. Coordinators also split when they grow.
- **`FokosDBMaterializationDO`** — keeps the latest image of each changed item of a table, and sends the changes to its GSIs and push targets. A partition exports its committed changes to it in the background, so a GSI is eventually consistent with its base table.
- **`FokosDBAutoAdminDO`** — holds the topology of each shard group of a table, for example its partitions or its coordinators. The `nsAdmin` table option names its binding.
- **`FokosDBTopologyCache`** — a cached Worker entrypoint that serves the topology of a shard group from its admin DO, so that callers do not all read the admin DO. The `topologyEntrypoint` table option names it.
- **`FokosDB`** — the client. It sends a single-partition operation to its partition, a multi-partition write to a coordinator, and drives a multi-partition read itself.
- **`FokosShardingRuntime`** — the sharding library that the partition, coordinator, and materialization DOs host. It owns identity, routing, route caches, repartition, and the alarm. The host class owns its schema and its operations.

Invariants:

- **`PartitionContext` travels in every RPC**, because Workers RPC cannot configure a DO when it starts. Get a DO namespace or stub only through `shared/do-stubs.ts`, because it applies the jurisdiction.
- **The DO name is the address of the data.** Never change its encoding.
- **`hashSplitN` and `rootTreesN` must never change after initialization.** A change breaks routing and loses data.
- A partition ID is opaque. Read it only through `PartitionIdHelper`.

### Repartition

A partition past its size cap splits by hash into children, and then becomes a router. A routingHash that is too large moves to a range tree of its own (promotion), which splits by `(hashKey, sortKey)`. The children import their data in the background. During the import, a read goes through the source and other operations answer `partition_migrating`.

**NOTE**: Once Durable Objects offer a native fork, clone, or snapshot of storage, the whole migration flow can go. The split stays the same.

## Transactions (2PC)

The model follows the DynamoDB papers: [ATC 2023, Idziorek et al.](https://www.usenix.org/system/files/atc23-idziorek.pdf) and [ATC 2022, Elhemali et al.](https://www.usenix.org/system/files/atc22-elhemali.pdf)

- Coordinator states are `CREATED → PREPARING → COMMITTING → COMMITTED`, or `CREATED → PREPARING → CANCELLING → CANCELLED`. Every transition writes to SQLite BEFORE it sends an RPC.
- **`COMMITTING` is the point of no return.** The coordinator never goes from `COMMITTING` to `CANCELLING`.
- More than one drive of one transaction can run at the same time. Only the stored state decides what a drive sends.
- `prepare`, `commit` and `cancel` are idempotent. The `items` table holds committed state only. A non-transactional write to a locked item is REFUSED, not delayed.
- A read transaction reads twice and aborts with `read_conflict` when anything changed.
- Conditions, updates, projections, and filters travel as expression trees, never as SQL. Only a partition compiles them.
- Apply a terminal outcome through `dispatch("txCommit" | "txCancel", ...)`, never through inline SQL. Never release the locks of a transaction by its transaction ID alone, because that also deletes the copies of a moved key.

## Partition operations

- Every public RPC method of `FokosDBPartitionDO` is one `this.fokos.dispatch(op, ctx, req)` call. Declare each operation in `PartitionOps`.
- The runtime handles import and repartition state. The host never examines it, and never reads a `fokos_` table or a `__fokos/` key.
- **Never swallow a group error.** A failed part of a fan-out must make the whole operation fail, so the coordinator retries.
- A local handler is synchronous. Every local write commits before the first `await` of `dispatch`.

## Testing

Tests run in the real Workers runtime through `@cloudflare/vitest-plugin`. Each suite makes its own namespace with a `crypto.randomUUID()` prefix.

- Use `makeStub` (`test/partition-do/helpers.ts`) for an ordinary test. Use `TestPartition` only when the test drives a split, a migration, or a promotion.
- Property-based suites use `fast-check`. Read `test/property-based/README.md` before you add, change, or replay one.
- Do not use global fake timers. They can run a DO callback in the wrong I/O context.

## Where the detail lives

- `docs/adr/` — architecture decisions.
- `docs/agent-plans/` — one dated specification for each feature.
- `docs/ideas/` — proposals that are not decided yet.
- `docs/research/` — background research.
- `packages/fokosdb/formal/p/` — the P model of the transactions. `pnpm formal:p` runs it in Docker, `pnpm formal:p:slow` runs more schedules, and `pnpm formal:p --help` gives the usage. `pnpm test` does not run it. A `tcBug<Defect>` test case must fail with the monitor of its seeded defect, and every other test case must pass.
