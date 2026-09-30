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

`packages/fokosdb/src` has four parts. Convention keeps them apart, not the module system.

- `client/` — `db.ts` and the entry barrel. Published as `fokosdb/client`.
- `server/` — the two Durable Object classes. Published as `fokosdb/server`.
- `shared/` — what both sides use.
- `sharding/` — the sharding library, which uses no FokosDB module. `index-client.ts` is published as `fokosdb/sharding/client` (the caller side: `FokosShardingClient`, routing types, errors), and `index-server.ts` as `fokosdb/sharding/server` (the runtime, the store, the repartition flow, and a re-export of the client entry). `pnpm build` fails when the client entry reaches a runtime module.

**The client must never import a Durable Object class as a value.** That pulls the whole server implementation into `dist/client`. Use the type-only helpers in `shared/do-stubs.ts` and keep every class import `import type`. `pnpm build` enforces the rule, pins the packages the client may import, and holds the client bundle under a size budget.

A cohesive folder stays whole inside `shared/` even when only one side uses it. An entry pulls in only the modules it names.

## Architecture

- **`PartitionDO`** (`src/server/do-partition.ts`) — holds items in SQLite, one DO per partition shard. It serves single-item reads and writes, acts as a resource manager in 2PC, and splits itself when it grows past its cap. It hosts `FokosShardingRuntime` (`src/sharding/runtime.ts`): the runtime owns identity, routing, the route caches, the repartition flow, the read-through, and the alarm; the class owns its SQLite schema, its operations, its admission and split policy, its migration pages, and its TTL timer.
- **`TransactionCoordinatorDO`** (`src/server/do-transaction-coordinator.ts`) — the second host of the sharding runtime. It drives 2PC for a write transaction. The idempotency token is its route key, and its shard group is `fokos.tc.<tableName>`: `coordinatorRootsN` roots that split by hash when they grow past `hashSplitConditions.maxSizeMb`. A read transaction runs in the Worker instead.
- **`FokosDB`** (`src/client/db.ts`) — the client entry point. It sends every partition and coordinator RPC through a `FokosShardingClient` (`src/sharding/client.ts`), sends a multi-partition write to a coordinator, and drives a multi-partition read itself.

Every partition RPC answers a `FokosEnvelope<T>`: `value` is the result and `routing` is the route evidence. The client opens it and drops the internal hints, and `client/partition-info.ts` builds the public `PartitionInfo` from `routing.servedBy` and `routing.forwardCount`. An error a partition raises carries its `routing` as an own property, and `withFokosErrors` in `db.ts` turns it into the same public `meta` and drops the routing.

An item has a `hashKey`, an optional `sortKey` (default `""`), data as `Uint8Array | string`, a `version` that every write increments, and an optional TTL.

**`PartitionContext` travels in every RPC.** Workers RPC cannot configure a DO at instantiation, so the topology configuration goes with each request and the DO compares it with the one it stored. Never read `env[ctx.ns]` outside `shared/do-stubs.ts`: use `partitionNamespace`, `txCoordinatorNamespace`, or the stub helpers, because each one applies the configured jurisdiction.

## Partitions

- The partition shard group of a table is `fokos.p.<tableName>`. A DO name is `<shardGroup>~h.<root>[.<child>...]` or `<shardGroup>~r.<hk>.<start>.<end>`, and a shard group must not contain `~`. The DO name is the address of the partition data, so its encoding (`encodeRangeComponent`) must never change.
- `rootTreesN` root partitions exist at startup, and a hash of the hash key selects one. A partition ID is opaque: read it only through `PartitionIdHelper`.
- `FokosShardingClient` picks the root partition of a hash key on the caller side, sends the operation, and retries only by the retry policy of the caller. `walk` reads the tree and changes nothing; `destroy` fences and deletes every partition. Inside a DO the runtime resolves the owner of every key (`resolveOwner`, `owns`), plans the range frontier (`rangeVisits`), and forwards (`forward`, `forwardRangeVisit`). The host never makes a stub to a peer of its own class.
- **Hash split** — a partition past `hashSplitConditions.maxSizeMb` queues a split, creates `hashSplitN` children, becomes a router, and the children import their share in the background. Its runtime states are `queued`, `planned`, `cutover` and `completed`; `status` reports them as `split_queued`, `split_started` and `split_completed`.
- **Promotion** — one hash key past `hashSplitConditions.maxSizeMb * promotionFraction` (a setting of `PartitionDO.fokosConfig()`) moves into a range tree of its own, which then splits by sort key. A promotion candidate is signalled before a split, because an unfinished promotion blocks the split behind it. A lock on the key does not hold the promotion: the lock moves with the key.
- **`hashSplitN` and `rootTreesN` must never change after initialization.** A change breaks routing and loses data. `rangeSplitN` can change: it applies only to the range splits that start after the change.
- A partition refuses a write above 1.1 times its cap. A write that applies and a write that `#admit` refuses both start the split decision (`hooks().evaluateSplit`). The decision returns `false` at the floor, where no repartition can make the partition smaller: a hash leaf with no hash key in `items` or `pending_transactions`, a range leaf with no plan from `planRangeSplit`, or a coordinator with fewer than two tokens. A hash leaf with one hash key promotes that key. A hash leaf with two or more hash keys promotes its keys above the promotion threshold (up to 8, largest first) before it splits.

The repartition flow runs as the runtime job `source_repartition`. It initializes each child with `fokosInit` and cuts routing over, then each child pulls pages with `fokosMigrationPull`. The runtime moves the route overrides first; the host phase (`FokosMigrationHost`) then builds the item and pending-transaction pages and filters rows with the `belongsToTarget` predicate the runtime hands it. While a child imports, a read goes through the source and every other operation answers `partition_migrating`. A child in `awaiting_data` adds `attributes.importState: "awaiting_data"`: a partition that sent the request there because of its Bloom filter then resolves the key again without the filter.

**NOTE**: Once Durable Objects offer a native fork, clone, or snapshot of storage, the whole migration flow can go. The split stays the same.

## queryItems

`queryItems` returns one bounded page. A caller follows `cursor` until it is absent, and a page can hold no items and still carry a cursor. `select` is `"projection"` or `"count"`. A request can also carry a `filter` and a `projection`; SQLite evaluates both and JavaScript evaluates neither.

Four budgets bound one page: evaluated items (`limit`), evaluated bytes, response bytes (`maxResponseBytes`), and partition visits. `QueryPageBudget` (`shared/query/page-budget.ts`) carries them across the sub-queries in `FokosDB` and across the planned visits of `walkRangeVisits` in `do-partition.ts`.

## Transactions (2PC)

The model follows the DynamoDB papers: [ATC 2023, Idziorek et al.](https://www.usenix.org/system/files/atc23-idziorek.pdf) and [ATC 2022, Elhemali et al.](https://www.usenix.org/system/files/atc22-elhemali.pdf)

- Coordinator states are `CREATED → PREPARING → PREPARED → COMMITTING → COMMITTED`, or `→ CANCELLING → CANCELLED`. Every transition writes to SQLite BEFORE it sends an RPC.
- **`PREPARED` is the point of no return.** A prepared transaction must commit, and the coordinator never goes from `PREPARED` to `CANCELLING`.
- More than one drive of one transaction can run at the same time: the request, a retry with the same token, and the `tx_recovery` job. Only the SQL writes of `PREPARED` and `CANCELLING` decide, and both require `PREPARING`. `runCommit` sends commits only when the stored state is `PREPARED` or `COMMITTING`, and `runCancel` sends cancels only when it is `CANCELLING`. A fan-out that follows a decision that lost breaks atomicity: the cancel releases a lock, and the participant answers the later commit with the idempotent success.
- `prepare`, `commit` and `cancel` are idempotent. The `items` table holds committed state only. `pending_transactions` holds one lock row for each key of an in-flight transaction, and `pending_tx` holds one row for each transaction: its timestamp, its coordinator, `created_at`, `guarded_at` and `next_recovery_at`. A `pending_tx` row stays while any lock row of its transaction stays, copies of a moved key included.
- A non-transactional write to a locked item is REFUSED, not delayed.
- A read transaction reads twice and compares `found`, `version` and the partition's `deleteRevision`. Any change aborts it with `read_conflict`.
- `clientRequestToken` routes to the coordinator and gives idempotency. `db.ts` always sends a token, and generates one when the caller gave none. A retry must use the same `coordinatorRootsN`.
- Every durable transition of the coordinator runs `fokos.owns(token)` inside its `transactionSync`. After a split cutover the transition writes nothing and throws `partition_migrating`, and `db.ts` retries with the same token until the child resumes the transaction.
- A lock row stores a `CoordinatorRef` (the `doName` of its coordinator and the token) as one JSON column. The stale-recovery job gets the stub with `txCoordinatorStubForParticipant`, from the `nsTx` and the jurisdiction of the partition, and calls `recoverTransactionForParticipant`. The coordinator routes the call with its own stored route context, and a coordinator that has split forwards the call. The coordinator answers from its ledger and does not drive the transaction in the call: for a transaction that is not complete, it sets `next_recovery_at` of the transaction to now, makes its `tx_recovery` job due now, and answers `driving`.
- The coordinator's `tx_recovery` job works like the stale job of a partition. `tc_state.next_recovery_at` is `created_at + staleTransactionMs` at the insert, and NULL after the completion. The `deadline()` of the job and its batch come from the partial index `idx_tc_state_recovery` (`WHERE completed_at IS NULL`). Each iteration of the step claims the transaction with the earliest `next_recovery_at` that was due when the step started, moves its `next_recovery_at` forward with `nextRecoveryAt`, and then drives it. A transaction that stays goes behind the others, and a transaction that the step does not reach keeps its place. Each drive gets at most `fanoutRequestBudgetMs` of the step budget `alarmRecoveryBudgetMs`. A terminal state must always have `completed_at`, because the index depends on it.

## Rules for PartitionDO operations

Every public RPC method of `PartitionDO` is one `this.fokos.dispatch(op, ctx, req)` call, and the operation name is the method name: the runtime forwards by calling the method of that name on the target stub. The operations are declared once in `PartitionOps` and registered in `operations()`, so a call that names one operation and passes the request of another does not compile.

The runtime runs the two concurrent state machines: **import** (a target that still catches up from its source) and **repartition** (a source that now routes to its targets). A descriptor tells the runtime what to do, and the host never examines either state itself.

- **Import** — `whileMigrating: "throw"` answers `partition_migrating` while this partition imports. `"read_source"` runs the same operation on the source through `fokosExecuteLocal`, and needs `readOnly: true` and a `point` or `range` shape. Only `apiGetItem` and `apiQueryItems` read through.
- **Shape** — the shape routes the request: `point` (the item RPCs), `group` (`txPrepare`, `txCommit`, `txCancel`, `txReadForTransaction`), `single_owner` (`txReadSnapshot`, `txExecuteSingleShot`), `range` (`apiQueryItems`), and `local` (`status` and the two debug operations).
- **Never swallow a group error** — a `group` with `failurePolicy: "attempt_all"` runs every remote group and throws `partition_fanout_failed` when one failed, so the coordinator stays non-terminal and retries. `txPrepare` and `txReadForTransaction` are `fail_fast`.
- **One call per owner** — every shape resolves with the hash arena, the Bloom filter and the learned range slices, and falls back after a cache miss. In one `group` dispatch, each owner must receive its keys in one sub-request, or `txCommit` throws `commit_keyset_mismatch`. The speculation guard drops a Bloom hit that would break this rule, and a `group` or `single_owner` retry turns Bloom off. A new `group` operation must be safe to repeat for each key after a partial fan-out. `docs/agent-plans/2026-09-27-learned-routes-for-every-dispatch-shape.md` has the rules.
- A `local` handler of a `point`, `group`, `single_owner` or `range` operation is synchronous, and every local write commits before the first `await` of `dispatch`. A handler reports what its response does not carry with `call.signal(...)`: `evaluateSplit`, `promotionCandidates` (`signalGrowth`), and `jobs` for the stale-transaction deadline after an accepted prepare.
- The host reaches its own facts through `this.fokos.identity()`, `policy()`, `routeContext()`, `lifecycle()` and `owns(key)`. It reads no `fokos_` table and no `__fokos/` key.

### Background recovery (stale-TX job)

Stale-transaction recovery is the host job `stale_tx_recovery` in `hooks().jobs`. Its `canRun` is `canSweepLocally()`, which is false on a router, on a target that is `awaiting_data` or `importing`, and behind the destroy fence, because none of these owns complete lock state. Its `deadline()` is the earliest `next_recovery_at` of an unguarded transaction, so the alarm also covers a lock that a restart left behind. Prepare sets `next_recovery_at` to `created_at + staleTransactionMs`. Each step claims the due transactions and moves `next_recovery_at` of each one forward before it calls the coordinator: half the age of the transaction, at least `staleTransactionMs` and at most 30 s. A lock that stays, because the coordinator answers `driving` or fails, therefore does not keep the deadline in the past. A transaction whose rows here are all copies of a moved key gets no coordinator call. One step starts coordinator calls for at most 10 s, because the passes of the scheduler run one at a time. The TTL sweep uses the same guard, stays an in-memory timer that every RPC arms, and registers no job.

A `not_found` result cancels an owned lock that is no older than `IDEMPOTENCY_WINDOW_MS`. It quarantines an older owned lock: set `guarded_at` on its `pending_tx` row, log the lock-age guard error once, and wait for `debugForceResolveTransaction`. The guard covers each lock of the transaction here, copies included, and the migration carries it to the new owner. A guarded transaction must stay out of the stale scan and out of its alarm scheduling.

Apply a terminal outcome through `this.fokos.dispatch("txCommit" | "txCancel", this.fokos.routeContext(), ...)`, never through inline SQL and never by calling the participant directly. Recovery reads the rows and their owners again after the coordinator call, and applies the outcome to the rows that this partition owns now. `debugForceResolveTransaction` sends every row it holds through `dispatch`, so the current owner of each key applies the outcome, and it changes only the owned rows here.

A promotion moves the locks of its key: the `pending_tx` stream copies them to the range root, and the source keeps the copies until the completion transaction deletes them. A split router keeps its copies in the same way. No local decision applies to a copy. `commitLocal`, the cancel release, and the quarantine change owned rows only, one statement per key. The stale job reads the keys of each claimed transaction, and it skips a transaction whose rows here are all copies. Never release the locks of a transaction by its transaction id alone: that delete also removes the copies.

## Testing

Tests run in the real Workers runtime through `@cloudflare/vitest-plugin`. Each suite makes its own namespace with a `crypto.randomUUID()` prefix.

- `test/partition-do/` holds one file per `PartitionDO` behaviour, `test/transactions/` the transaction suites, and `test/repartition/` the repartition flows.
- Use `makeStub` (`test/partition-do/helpers.ts`) for an ordinary test. Use `TestPartition` (`partition-harness.ts`) only when the test drives a split, a migration, or a promotion, with `triggerHashSplit`, `triggerRangeSplit`, `splitHash`, `splitRange`, `makeRangeRoot`, `runAlarm` and `drainUntil`.
- Property-based suites are in `test/property-based/` and use `fast-check`. Read that directory before you add or change one: each suite says at its top what it covers, and its shared modules say how a run is sized and how a failure replays.
- Global fake timers can run a DO background callback in the wrong I/O context. Lifecycle tests use real timers and the scheduled-alarm test APIs instead.

## Where the detail lives

- `docs/adr/` — architecture decisions.
- `docs/agent-plans/` — one dated specification per feature. Read the matching one before you change that feature.
- `docs/ideas/` — proposals that are not decided yet.
