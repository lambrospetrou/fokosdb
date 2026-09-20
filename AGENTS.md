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

- **`PartitionDO`** (`src/server/do-partition.ts`) — holds items in SQLite, one DO per partition shard. It serves single-item reads and writes, acts as a resource manager in 2PC, and splits itself when it grows past its cap. It hosts `FokosShardingRuntime` (`src/sharding/runtime.ts`): the runtime owns identity, routing, the route caches, the repartition flow, the read-through, and the alarm; the class owns its SQLite schema, its operations, its admission and split policy, its migration pages, and its TTL timer.
- **`TransactionCoordinatorDO`** (`src/server/do-transaction-coordinator.ts`) — one DO per write transaction, named by the idempotency token. It drives 2PC. A read transaction runs in the Worker instead.
- **`FokosDB`** (`src/client/db.ts`) — the client entry point. It routes with `FokosRouter`, sends a multi-partition write to a coordinator, and drives a multi-partition read itself.

Every partition RPC answers a `FokosEnvelope<T>`: `value` is the result and `routing` is the route evidence. `FokosRouter.unwrap` opens it, and `client/partition-info.ts` builds the public `PartitionInfo` from `routing.servedBy` and `routing.forwardCount`. An error a partition raises carries its `routing` as an own property, and `withFokosErrors` in `db.ts` turns it into the same public `meta` and drops the routing.

An item has a `hashKey`, an optional `sortKey` (default `""`), data as `Uint8Array | string`, a `version` that every write increments, and an optional TTL.

**`PartitionContext` travels in every RPC.** Workers RPC cannot configure a DO at instantiation, so the topology configuration goes with each request and the DO compares it with the one it stored. Never read `env[ctx.ns]` outside `shared/do-stubs.ts`: use `partitionNamespace`, `txCoordinatorNamespace`, or the stub helpers, because each one applies the configured jurisdiction.

## Partitions

- `rootTreesN` root partitions exist at startup, and a hash of the hash key selects one. A partition ID is opaque: read it only through `PartitionIdHelper`.
- `FokosRouter` picks the root partition of a hash key on the client. Inside a DO the runtime resolves the owner of every key (`resolveOwner`, `owns`), plans the range frontier (`rangeVisits`), and forwards (`forward`, `forwardRangeVisit`). The host never makes a stub to a peer of its own class.
- **Hash split** — a partition past `hashSplitConditions.maxSizeMb` queues a split, creates `hashSplitN` children, becomes a router, and the children import their share in the background. Its runtime states are `queued`, `planned`, `cutover` and `completed`; `status` reports them as `split_queued`, `split_started` and `split_completed`.
- **Promotion** — one hash key past `hashSplitConditions.maxSizeMb * RANGE_PROMOTION_FRACTION` moves into a range tree of its own, which then splits by sort key. A promotion candidate is signalled before a split, because an unfinished promotion blocks the split behind it.
- **`splitN` must never change after initialization.** A change breaks routing and loses data.
- A partition refuses a write above 1.1 times its cap, and only a write that applies can queue the split that brings it back under.

The repartition flow runs as the runtime job `source_repartition`. It initializes each child with `fokosInit` and cuts routing over, then each child pulls pages with `fokosMigrationPull`. The runtime moves the route overrides first; the host phase (`FokosMigrationHost`) then builds the item and pending-transaction pages and filters rows with the `belongsToTarget` predicate the runtime hands it. While a child imports, a read goes through the source and every other operation answers `partition_migrating`.

**NOTE**: Once Durable Objects offer a native fork, clone, or snapshot of storage, the whole migration flow can go. The split stays the same.

## queryItems

`queryItems` returns one bounded page. A caller follows `cursor` until it is absent, and a page can hold no items and still carry a cursor. `select` is `"projection"` or `"count"`. A request can also carry a `filter` and a `projection`; SQLite evaluates both and JavaScript evaluates neither.

Four budgets bound one page: evaluated items (`limit`), evaluated bytes, response bytes (`maxResponseBytes`), and partition visits. `QueryPageBudget` (`shared/query/page-budget.ts`) carries them across the sub-queries in `FokosDB` and across the planned visits of `walkRangeVisits` in `do-partition.ts`.

## Transactions (2PC)

The model follows the DynamoDB papers: [ATC 2023, Idziorek et al.](https://www.usenix.org/system/files/atc23-idziorek.pdf) and [ATC 2022, Elhemali et al.](https://www.usenix.org/system/files/atc22-elhemali.pdf)

- Coordinator states are `CREATED → PREPARING → PREPARED → COMMITTING → COMMITTED`, or `→ CANCELLING → CANCELLED`. Every transition writes to SQLite BEFORE it sends an RPC.
- **`PREPARED` is the point of no return.** A prepared transaction must commit, and the coordinator never goes from `PREPARED` to `CANCELLING`.
- `prepare`, `commit` and `cancel` are idempotent. The `items` table holds committed state only, and `pending_transactions` holds the locks of the in-flight transactions.
- A non-transactional write to a locked item is REFUSED, not delayed.
- A read transaction reads twice and compares `found`, `version` and the partition's `deleteRevision`. Any change aborts it with `read_conflict`.
- `clientRequestToken` names the coordinator DO and gives idempotency. A retry must use the same coordinator pool size.

## Rules for PartitionDO operations

Every public RPC method of `PartitionDO` is one `this.fokos.dispatch(op, ctx, req)` call, and the operation name is the method name: the runtime forwards by calling the method of that name on the target stub. The operations are declared once in `PartitionOps` and registered in `operations()`, so a call that names one operation and passes the request of another does not compile.

The runtime runs the two concurrent state machines: **import** (a target that still catches up from its source) and **repartition** (a source that now routes to its targets). A descriptor tells the runtime what to do, and the host never examines either state itself.

- **Import** — `whileMigrating: "retry"` answers `partition_migrating` while this partition imports. `"read_source"` runs the same operation on the source through `fokosExecuteLocal`, and needs `readOnly: true` and a `point` or `range` shape. Only `apiGetItem` and `apiQueryItems` read through.
- **Shape** — the shape routes the request: `point` (the item RPCs), `group` (`txPrepare`, `txCommit`, `txCancel`, `txReadForTransaction`), `single_owner` (`txReadSnapshot`, `txExecuteSingleShot`), `range` (`apiQueryItems`), and `local` (`status` and the two debug operations).
- **Never swallow a group error** — a `group` with `failurePolicy: "attempt_all"` runs every remote group and throws `partition_fanout_failed` when one failed, so the coordinator stays non-terminal and retries. `txPrepare` and `txReadForTransaction` are `fail_fast`.
- `txCancel` releases by transaction id in `beforeForward`, which runs on every hop before the remote groups start, so a router between cutover and completion clears its own pre-cutover lock rows.
- A `local` handler of a `point`, `group`, `single_owner` or `range` operation is synchronous, and every local write commits before the first `await` of `dispatch`. A handler reports what its response does not carry with `call.signal(...)`: `evaluateSplit`, `promotionCandidates` (`signalGrowth`), `repartitionUnblocked` after a commit or a cancel, and `jobs` for the stale-transaction deadline after an accepted prepare.
- The host reaches its own facts through `this.fokos.identity()`, `policy()`, `routeContext()`, `lifecycle()` and `owns(key)`. It reads no `fokos_` table and no `__fokos/` key.

### Background recovery (stale-TX job)

Stale-transaction recovery is the host job `stale_tx_recovery` in `hooks().jobs`. Its `canRun` is `canSweepLocally()`, which is false on a router, on a target that is `awaiting_data` or `importing`, and behind the destroy fence, because none of these owns complete lock state. Its `deadline()` is the oldest unguarded lock plus `fokosStaleTransactionMs()`, so the alarm also covers a lock that a restart left behind. The TTL sweep uses the same guard, stays an in-memory timer that every RPC arms, and registers no job.

A `not_found` result has three paths. Delete the lock directly when all its keys route away. Cancel an owned lock that is no older than `IDEMPOTENCY_WINDOW_MS`. Quarantine an older owned lock by setting `guarded_at`, log the lock-age guard error once, and wait for `debugForceResolveTransaction`. A guarded transaction must stay out of the stale scan and out of its alarm scheduling.

Apply a terminal outcome through `this.fokos.dispatch("txCommit" | "txCancel", this.fokos.routeContext(), ...)`, never through inline SQL and never by calling the participant directly. `dispatch` resolves the owner of every key again, so a key that moved to a child since the lock was written is applied there. `debugForceResolveTransaction` follows the same rule.

## Testing

Tests run in the real Workers runtime through `@cloudflare/vitest-pool-workers`. Each suite makes its own namespace with a `crypto.randomUUID()` prefix.

- `test/partition-do/` holds one file per `PartitionDO` behaviour, `test/transactions/` the transaction suites, and `test/repartition/` the repartition flows.
- Use `makeStub` (`test/partition-do/helpers.ts`) for an ordinary test. Use `TestPartition` (`partition-harness.ts`) only when the test drives a split, a migration, or a promotion, with `triggerHashSplit`, `triggerRangeSplit`, `splitHash`, `splitRange`, `makeRangeRoot`, `runAlarm` and `drainUntil`.
- Property-based suites are in `test/property-based/` and use `fast-check`. Read that directory before you add or change one: each suite says at its top what it covers, and its shared modules say how a run is sized and how a failure replays.
- Global fake timers can run a DO background callback in the wrong I/O context. Lifecycle tests use real timers and the scheduled-alarm test APIs instead.

## Where the detail lives

- `docs/adr/` — architecture decisions.
- `docs/agent-plans/` — one dated specification per feature. Read the matching one before you change that feature.
- `docs/ideas/` — proposals that are not decided yet.
