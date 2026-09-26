# RFC — FokosShardingClient: one caller layer that resolves, sends, and retries for the sharding runtime

**State:** Draft
**Date:** 2026-09-26
**Author:** Lambros Petrou

**Status:** Nothing is built. `FokosRouter` in `packages/fokosdb/src/sharding/router.ts` is the only Worker-side
routing code today.

## Table of contents

- [1. Overview and context](#1-overview-and-context)
- [2. Goals and requirements](#2-goals-and-requirements)
- [3. Milestones](#3-milestones)
- [4. Proposed solution](#4-proposed-solution)
  - [4.1 High-level overview](#41-high-level-overview)
  - [4.2 Technical details](#42-technical-details)
  - [4.3 Future extensions](#43-future-extensions)
- [5. Alternative options](#5-alternative-options)
- [6. Frequently asked questions](#6-frequently-asked-questions)
- [7. References](#7-references)

## 1. Overview and context

`FokosShardingRuntime` (`docs/agent-plans/2026-09-19-fokos-sharding-runtime.md`) moved all routing inside a
Durable Object into `src/sharding/`. A partition resolves the owner of each key, forwards, and learns deeper
routes from the response envelope. The runtime is usable without FokosDB. `examples/http-api/src/demo2/` has two
hosts: a counter and a full-text search index.

The caller side has no equivalent layer. A Worker that calls a partition repeats five steps at each call site:

```ts
const ctx = router.rootContext(hashKey);                     // 1. always the root partition
const stub = partitionStubByName(env, ctx, ctx.doName);      // 2. the caller makes the stub
const envelope = await stub.apiGetItem(ctx, req);            // 3. the method name is the operation name
const { value, routing } = router.unwrap(envelope);          // 4. drops the internal route hints
// 5. retry and error handling, different at each call site
```

### 1.1 The current flows

| Flow | Call site | What it does |
| --- | --- | --- |
| Point operation | `FokosDB` `#putItem`, `#getItem`, `#deleteItem`; demo `increment` and `addDoc` | Root context, one RPC, unwrap. `FokosDB` does not retry. The demos retry when the error text contains `"partition_migrating"`. |
| Range operation | `FokosDB` `#queryItems`; demo `search` | Root context. The partitions plan the range frontier. |
| Write transaction | `FokosDB` `#transactWriteItems` | Each item carries its root context. `singlePartitionTarget` compares the root `doName`s to select the fast path. The coordinator call retries `partition_migrating` with `tryWhile` until a deadline. The coordinators send to their participants with their own `tryWhile` retries. |
| Read transaction | `FokosDB` `#readTransaction`, `#readSnapshotFastPath` | Groups the items by root `doName`, then runs two phases. Each phase retries any error up to 5 times. The fast path retries a retryable error up to 3 times. |
| Tree inspection | demo `collectTree`; `test/sharding/counter-table.ts` `tree` | Each one reads its host statistics and follows the children by hand. |
| Destroy | `FokosRouter.walk` | Used by `FokosDB.destroy` and by the demo `resetTree`. |
| Errors | `withFokosErrors` in `db.ts` | Turns the routing that a partition attaches to an error into the public `meta`. |

### 1.2 The problems

1. **The caller learns nothing.** `FokosRouter.unwrap` removes `_rangeAncestors`. Every request enters at a
   root. A key under a hash tree of depth D, or under a range tree, pays every router hop on every request.
2. **No single place makes stubs.** `FokosDB` uses `partitionStubByName`, which applies the jurisdiction and the
   location hint. The demos use `idFromName`, which ignores the jurisdiction. The runtime already takes a
   `stub(ctx, doName)` callback from its host. The caller side has no such callback.
3. **Each call site has its own retry.** The rules differ, and no single place names them.
4. **The fast-path hint is coarse.** Two keys under one root but in two different leaves pass the client check.
   The partition then answers `not_applicable`, and the transaction pays one extra round trip.
5. **The caller must remember the entry partition.** A router does not list itself in `servedBy`. So the demo
   `traceOf` needs the entry reference from the caller.
6. **`walk` means destroy.** `FokosRouter.walk` fences each partition with `fokosPrepareDestroy`. The fence stops
   the partition for good. A caller that only reads the tree cannot use it. So the demos and the tests write
   their own traversal.
7. **A write can wait for as long as a promotion is held.** The problem has four steps:
   1. A hash partition resolves key `k`. Its promotion Bloom filter gives a false positive for `k`.
   2. The partition forwards the write to the range root of `k`. The promotion of `k` is planned but held before
      its cutover, so that range root is a target in `awaiting_data`.
   3. The target refuses the write with `partition_migrating`. `#fallbackAfterMiss` does not fall back on that
      code, so the hash partition returns the error.
   4. Every retry takes the same path until the source cuts over. A transaction lock can hold the cutover. A lock
      in quarantine holds it until `debugForceResolveTransaction`.

This document solves problems 2, 3, 5, 6, and 7. It also adds the caller layer where a caller-side route cache
connects later. That cache solves problems 1 and 4. The cache is separate work (section 4.3.1), and it depends on
the fix of problem 7.

### Glossary

| Term | Meaning in this document |
| --- | --- |
| client | One `FokosShardingClient` instance. It serves one shard group. |
| caller | The code that uses a client: a Worker, `FokosDB`, or a Durable Object that calls another shard group. |
| entry | The partition that receives the first RPC of a request. Every resolve in this document returns a root. |
| retry | Send the same request to the same entry again, after a delay, when the retry policy of the caller allows it. |
| group | A set of route keys that resolve to one entry. |
| owner, router | As in the runtime spec: a partition that serves its keys locally, or one that forwards every key after a split cutover. |
| source, target | As in the runtime spec: the partition that repartitions, and a partition that receives a slice of it. |

## 2. Goals and requirements

### 2.1 In scope

- `FokosShardingClient<TPolicy, Ops>`: resolve an entry, make the stub, send the operation, retry by the policy
  of the caller, unwrap, and report the cost of the call.
- `resolveAll(keys)`: groups unrelated route keys by entry, for batch operations and for transactions.
- `walk()`: a read-only traversal of the partitions, separate from destroy.
- `destroy()`: the fenced destroy traversal, with a name that says what it does.
- `FokosDB`, the demos, and the tests use the client. Their own routing, retry, and traversal code goes. Their
  retry rules move into retry policies without change.
- The runtime counts every outbound RPC in `forwardCount`, and also an RPC that failed without routing.
- The request to the coordinator carries the table topology once, and each item carries its keys only.
- A target before its cutover sends a request to its source, and does not refuse it.

### 2.2 Out of scope

- **The caller-side route cache, and the fallback after a stale cache entry.** Section 4.3.1 records the rules
  that the cache work must keep. The client of this document has the place where the cache connects: every entry
  comes from `resolve`, `resolveRange`, or `resolveAll`.
- **Batch item operations.** `resolveAll` supports them. The operations themselves are separate work.
- **A shared route spec.** Each call passes its route key explicitly. A spec that both the host `operations()`
  and the caller import, so that the key extractor exists once, is a later change.

### 2.3 Requirements

- **The client must never import a Durable Object class as a value.** The rule of `AGENTS.md` applies. The client
  goes into the client bundle of `FokosDB`.
- **The stub callback has the runtime's contract.** It is `FokosRuntimeOptions.stub`: the host applies the
  binding, the jurisdiction, and the location hint.
- **The client must send the same RPCs as now.** It sends each request to the root that the current code
  selects. It retries by the same rules as the current call site.
- **The client must not retry by itself.** It retries only when the caller gives a retry policy. The client cannot
  know if an operation is idempotent (section 4.2.4).
- **`walk` must not change partition state.** It makes no fence. It creates no identity, so it bootstraps no root.
  It can start a cold Durable Object, and the start writes the schema of that object (section 4.2.6).
- **The internal hints stop at the client.** A result or an error that leaves the client carries
  `FokosPublicRouting`, without `_rangeAncestors`.

## 3. Milestones

Each milestone ships and is useful alone.

### M1 — The client, the retry policy, and the walk and destroy split

- Add `FokosShardingClient` with `resolve`, `resolveRange`, `resolveAll`, `point`, `range`, `send`, `walk`, and
  `destroy`. Every resolve returns a root context.
- Add `FokosRetryPolicy`. Move the retry rules of `FokosDB` and of the demos into policies.
- Count every outbound RPC in `forwardCount` (section 4.2.4).
- Add `role` to `FokosStatusPage`, so that `walk` reads the role and does not derive it from repartition states.
- Remove `FokosRouter.walk`. `FokosRouter` keeps `rootContext`, `allRoots`, and `unwrap`.
- Move `FokosDB`, the demos, and `test/sharding/counter-table.ts` to the client. `FokosDB.destroy` calls
  `destroy()` on its coordinator client, then on its partition client.
- Delivers: one place for stubs and retries, a read-only walk, the cost of each call, and the same RPCs as now.

### M2 — The coordinator request carries the table topology once

- `initiateWrite` takes the table topology once, and items without `partitionContext`. This is a breaking change
  of the coordinator request (section 4.2.9).
- The coordinator resolves and groups the participants with its own `FokosShardingClient`. It sends to them with
  `send`.
- This milestone is separate because it changes the coordinator protocol code in
  `do-transaction-coordinator.ts`.

### M3 — A target before its cutover asks its source

- A target in `awaiting_data` sends every request that its lifecycle gate receives to its source, through
  `fokosExecuteLocal`. The source runs the request while it still owns the slice (section 4.2.10).
- A source that has cut over answers a write with `repartition_cut_over`. The target takes that answer as the
  start notification. It starts its import and answers `partition_migrating`.
- Remove the `repartition_not_cut_over` fallbacks of `#fallbackAfterMiss` and `#forwardRangeVisit`. After this
  change, no request path raises that code.
- This milestone comes last because it changes the runtime and not the client. The caller-side route cache
  depends on it.
- Delivers: the fix of problem 7. The runtime also answers correctly when any hint names a target before its
  cutover.

## 4. Proposed solution

### 4.1 High-level overview

A caller makes one `FokosShardingClient` for each shard group. The client has the topology, the range config,
the policy, and a stub callback. It can also have a retry policy. The caller names the operation, the route key,
and the request. The client does the rest:

1. It resolves the entry. In this document, the entry is the root of the hash key.
2. It gets the stub from the callback. It calls the method that has the name of the operation.
3. On an error, it asks the retry policy. When the policy allows it, the client sends again to the same entry.
4. It removes the internal hints from the routing.
5. It returns the value, the public routing, the entry, and the cost of the call.

```
 Worker isolate                                         Durable Objects
 ┌──────────────────────────────────────┐
 │ caller ─► FokosShardingClient        │
 │             │ resolve: root of the hash key
 │             │ stub(ctx, doName)      │
 │             └─ send ────────────────────────► entry partition ─► ... ─► owner
 │                 unwrap ◄─ envelope ◄──────────────────────────────────┘
 │                 retry: by the policy of the caller, to the same entry
 └──────────────────────────────────────┘
```

A batch or a transaction calls `resolveAll`. The client groups the route keys by entry and keeps the item
positions. The caller sends one request for each group.

`walk` reads the tree and changes nothing. `destroy` fences and deletes every partition.

A target before its cutover does not refuse a request. It sends the request to its source. The source runs the
request while it owns the slice. After the cutover, the source tells the target to start its import.

### 4.2 Technical details

#### 4.2.1 Placement

- `src/sharding/client.ts` holds `FokosShardingClient` and `FokosRetryPolicy`.
- `fokosdb/sharding` exports them. `fokosdb/client` re-exports them, as it re-exports `FokosRouter` now.
- `db.ts` imports them by path. The `check-client-bundle` plugin and the size budget in `tsdown.config.ts` apply.
- The module does not import `runtime.ts`, `repartition-flow.ts`, or `sharding-store.ts` as a value. It uses
  `tryWhile` from `durable-utils/retries`. Both entries can import that package.

#### 4.2.2 The client API

```ts
export type FokosRetryPolicy = {
	/** Called after each failed attempt. `nextAttempt` is 2 for the first retry. */
	shouldRetry(this: void, err: unknown, nextAttempt: number): boolean;
	/** Default 100. Must be less than `maxDelayMs`, as `tryWhile` requires. */
	baseDelayMs?: number;
	/** Default 2_000. */
	maxDelayMs?: number;
};

export type FokosCallOptions = {
	/** Replaces the retry policy of the client for this call. */
	retry?: FokosRetryPolicy;
};

export type FokosShardingClientOptions<TPolicy> = {
	topology: FokosTopology;
	rangeConfig: FokosRangeConfig;
	policy: TPolicy;
	/** The contract of `FokosRuntimeOptions.stub`. The stub must also have the `FokosShardingRpc` methods. */
	stub(this: void, ctx: FokosRouteContext<TPolicy>, doName: string): DurableObjectStub;
	/** Absent: no retry, as for the current point operations of `FokosDB`. */
	retry?: FokosRetryPolicy;
};

/** The cost of one client call over every attempt. */
export type FokosCallCost = {
	/** The RPCs that the client sent: the first send and each retry. */
	clientRpcs: number;
	/** The sum of `forwardCount` over every attempt, the failed attempts included. */
	totalForwardCount: number;
};

export type FokosCallResult<T> = FokosCallCost & { value: T; routing: FokosPublicRouting; entry: FokosPartitionRef };

export type FokosResolvedGroup<TPolicy> = { ctx: FokosRouteContext<TPolicy>; indexes: number[] };

export class FokosShardingClient<TPolicy, Ops extends FokosOperationSpec> {
	constructor(opts: FokosShardingClientOptions<TPolicy>);

	// Resolve only. No I/O.
	resolve(key: RouteKey): FokosRouteContext<TPolicy>;
	resolveRange(input: FokosRangeInput): FokosRouteContext<TPolicy>;
	resolveAll(keys: readonly RouteKey[]): FokosResolvedGroup<TPolicy>[];

	// Resolve, send, retry, unwrap.
	point<K extends keyof Ops & string>(
		op: K,
		key: RouteKey,
		req: Ops[K]["req"],
		opts?: FokosCallOptions,
	): Promise<FokosCallResult<Ops[K]["res"]>>;
	range<K extends keyof Ops & string>(
		op: K,
		input: FokosRangeInput,
		req: Ops[K]["req"],
		opts?: FokosCallOptions,
	): Promise<FokosCallResult<Ops[K]["res"]>>;
	/** Sends to an entry that the caller selected. `keys` are the route keys of the request. */
	send<K extends keyof Ops & string>(
		op: K,
		entry: FokosRouteContext<TPolicy>,
		req: Ops[K]["req"],
		keys: readonly RouteKey[],
		opts?: FokosCallOptions,
	): Promise<FokosCallResult<Ops[K]["res"]>>;

	walk(opts?: { scope?: "all" | "owners" }): AsyncGenerator<FokosWalkNode<TPolicy>>;
	destroy(opts?: { onDestroyed?(ref: FokosPartitionRef): void }): Promise<void>;
}
```

- The client calls the stub method that has the name of the operation. The runtime already needs this rule,
  because it forwards by that name. The typed `Ops` spec is the one that the host gives the runtime.
- The constructor validates the topology and the range config, as `FokosRouter` does.
- A client is cheap to make. A caller can make one for each request.
- `range` sends a `FokosRangeInput` for routing. The request itself is the host type, and the client does not
  read it.
- `send` takes the route keys of the request. This document does not read them. The route cache uses them to
  learn (section 4.3.1). So the signature does not change when the cache arrives.

A call from a Worker to the search demo host:

```ts
const search = new FokosShardingClient<SearchPolicy, SearchOps>({
	topology: { shardGroup: "search_demo", rootTreesN: 1, hashSplitN: 2 },
	rangeConfig: { rangeSplitN: 2, rangeAncestors: { fromRoot: 0, fromLeaf: 3 } },
	policy: { promoteAtDocs: 5, rangeSplitAtDocs: 8 },
	stub: (_ctx, doName) => env.SEARCH_PARTITION_DO.getByName(doName),
});

const deadline = Date.now() + 15_000;
const migrating = {
	shouldRetry: (err: unknown) => String(err).includes("partition_migrating") && Date.now() < deadline,
};
const { value, routing, entry } = await search.point("addDoc", { hashKey, sortKey }, req, { retry: migrating });
const hits = await search.range("search", { hashKey, interval, descending: true }, req);
```

#### 4.2.3 Resolution

- `resolve(key)` returns `FokosRouter.rootContext(key.hashKey)`.
- `resolveRange(input)` returns the root context of `input.hashKey`. The entry partition plans the frontier, as
  now.
- `resolveAll(keys)` resolves each key as `resolve` does. It groups the keys by the `doName` of the entry. It keeps
  the input order inside each group. `indexes` gives the input positions of the keys of each group.

A group is a hint. A group whose entry is a root can hold keys in different leaves. So the operation on the
partition must be a `group` or a `single_owner` shape, and the runtime fans the request out below the entry.
`resolveAll` does not apply to range requests. A request that spans the sort-key axis uses `resolveRange`.

The route cache changes only these three methods. The rest of the client does not change when the cache arrives.

#### 4.2.4 The send pipeline

For `point`, `range`, and `send`:

1. Get the stub with `stub(entry, entry.doName)`. Call `stub[op](entry, req)`.
2. On an error, call `shouldRetry(err, nextAttempt)` of the call policy, or else of the client policy. When it
   returns true, wait with a jittered backoff between `baseDelayMs` and `maxDelayMs`, then go back to step 1.
   `tryWhile` has this behavior.
3. Unwrap the result with `FokosRouter.unwrap`. On an error, replace `routing` with the public form, then throw
   the same error object.
4. Return `{ value, routing, entry }` and the cost fields.

**The caller decides the retry, because a refusal does not prove that nothing applied:**

- A `fail_fast` group runs its local part before it awaits the remote groups. When a remote group refuses, the
  local part has applied. The error has the code of the remote refusal.
- `initiateWrite` runs the whole transaction in its local handler. A state transition throws
  `partition_migrating` after a split moved the token. That can happen after the prepares were sent.

The runtime cannot tell a refusal in the lifecycle gate from a handler that threw part of the way through. The
caller knows if its operation is idempotent. So the caller gives a policy only for an operation that is safe to
send again, and the client has no default policy. The `FokosDB` operations that retry now are idempotent: the
reads, `txPrepare`, `txCommit`, `txCancel`, and `initiateWrite` with its idempotency token.

**The call cost.** `routing.forwardCount` comes from the final envelope only. It counts the
partition-to-partition RPCs of the response tree below the entry. It does not count the RPCs of the client.
Example: the first attempt fails with `partition_migrating`, and the retry succeeds at the root, which serves the
request locally. The call made 2 RPCs, and `forwardCount` is 0.

- `clientRpcs` counts every RPC that the client sent: the first send and each retry. The example gives 2.
- `totalForwardCount` is the sum of `forwardCount` over every attempt. A failed attempt adds the `forwardCount` of
  the routing on its error. An error without routing adds 0. The example gives 0.
- When the call throws, the client attaches the two fields to the error as own data properties, as the runtime
  attaches `routing`. `FokosDB` removes the two fields from a public error, as it removes `routing`.

**The runtime counts every outbound RPC.** Now, `RouteCollector.mergeForwarded` adds `forwardCount + 1` only when
the answer carries routing. So a forward that fails without routing is not counted. Example: a router forwards to
a hinted child without an identity, then falls back and succeeds. M1 changes the count:

- The runtime adds 1 to `forwardCount` when it sends an outbound RPC that carries the request. Two calls do this:
  `#forwardTo`, and the read-through call of the lifecycle gate.
- When the answer or the error carries routing, the runtime adds the `forwardCount` of that routing.
- A failed RPC without routing therefore counts 1.
- The read-through call handles an error as `#forwardTo` does. It merges the routing of the error, then it puts
  the routing of its own collector on the error.

`clientRpcs + totalForwardCount` is then the number of RPCs of the call, with two exceptions:

- When a `fail_fast` group throws, the count excludes the RPCs below the remote groups that did not answer yet.
  The RPC to each such group is in the count.
- When an attempt fails without routing, for example because its reply was lost, the count excludes the RPCs that
  the entry sent for that attempt.

The routing has three levels:

| Level | What it holds | Who reads it |
| --- | --- | --- |
| `envelope.routing` (`FokosRouting`) | Every partition that served a scope: `executed`, `merged`, or `read_through`. Each node has `hashDepth` and `rangeDepth`, and a range node also has `_rangeAncestors`. The routing also has `forwardCount` and `servedByTruncated`. | The client, before step 3. The route cache learns from it later. |
| `FokosPublicRouting` | The same list without `_rangeAncestors`. | The caller, in the result of `point`, `range`, and `send`. |
| `PartitionInfo` in the `FokosDB` `meta` | The executor node (`partitionInfoOf`), and the leaf nodes of `partitionMetas` (`leafPartitionInfo`). | The public `FokosDB` result. |

The runtime mapping in `withFokosErrors` stays in `FokosDB`. It is public error vocabulary, not routing.

#### 4.2.5 The retry policies of FokosDB and the demos

Each call site keeps its current rule and its current delays:

| Call site | Policy |
| --- | --- |
| `#putItem`, `#getItem`, `#deleteItem`, `#queryItems`, `#writeSingleShotFastPath` | None. |
| `#readSnapshotFastPath` | `isRuntimeRetryableError(err) && nextAttempt <= 3`, with `maxDelayMs: 3_000`. That is the `tryWhile` default that it uses now. |
| `#readTransaction`, both phases | `nextAttempt <= 5`. |
| coordinator call (`initiateWrite`) | `partition_migrating`, while `Date.now()` is before the deadline of `TX_COORDINATOR_MIGRATING_RETRY_MS`. |
| coordinator `txPrepare` | Every code except `partition_over_size`, and `nextAttempt <= 3`, as now. |
| coordinator `txCommit`, `txCancel` | The current rules of `runCommit` and `runCancel`. |
| demo writes | `partition_migrating`, for `RETRY_FOR_MS` (15 s). |

#### 4.2.6 walk

```ts
export type FokosWalkNode<TPolicy> = {
	ctx: FokosRouteContext<TPolicy>;
	kind: "hash" | "range";
	role: "owner" | "router";
	/** How the walk reached this partition. Null for a root. */
	parent: { ref: FokosPartitionRef; via: RepartitionKind } | null;
	importState: FokosImportState | null;
};
```

- `walk` is an async generator. It yields a parent before its children. A caller can stop early.
- It calls `fokosStatus` without `rootContext`, so it never creates an identity for an empty root. It skips a
  partition that has no identity.
- **The cost of a cold object.** A status call starts the Durable Object. The runtime constructor runs the sharding
  store migrations, and the host constructor runs its own migrations, for example in `PartitionDO`. So a walk
  writes the schema of every cold root it reaches. A walk always calls every root. On an unused table of
  `rootTreesN` roots, it starts `rootTreesN` objects, up to `FOKOS_HASH_PARTITIONS_MAX` (65,000). `destroy` has
  the same cost now. Section 4.3.6 gives an improvement.
- It removes duplicates by `doName`. One range root can be the promotion target of more than one hash partition.
- It makes no fence. The result is a best-effort snapshot. A split that runs during the walk can add a target that
  the walk does not see.
- `scope: "all"` (the default) yields every partition that has an identity: routers, owners, and targets before
  their cutover.
- `scope: "owners"` yields only the partitions that own a slice now. It excludes routers. It also excludes targets
  whose repartition has not cut over. The walk reads the repartition state from the status page of the parent.
- An owner can have children. A hash owner that promoted a key still owns its other keys.

**A walk is a topology snapshot, and never a source of row totals.** A caller must not sum the rows that the walked
partitions store, with either scope. The stored rows do not match ownership while a repartition runs:

- A split source keeps its item rows after the cutover. Its cleanup deletes nothing.
- A promotion source stays an owner of its other keys. It keeps the rows of the promoted key until its cleanup
  deletes them. Its target owns the same key after the cutover. A sum over the owners counts those rows twice.
- A target that imports after its cutover holds only part of its rows, and its source reads for it. A sum over the
  owners misses the rows that the target has not imported.

A caller that needs a total reads it through the host operations, which route by ownership. The demo `reconcile`
and the check of `settle` in `test/sharding/counter-table.ts` keep their own rules over the stored rows. The move
to `walk` in M1 does not change those rules.

`FokosStatusPage` gets a `role: "owner" | "router"` field, from `FokosLifecycle.role`.

#### 4.2.7 destroy

`destroy` is the current `FokosRouter.walk` traversal with one fixed visit:

1. For every root, and then for every target in post-order, call `fokosPrepareDestroy`. Pass the root context on a
   root only.
2. Read every `fokosStatus` page after the fence is set, and visit each target.
3. Call `fokosDestroy` on the partition. Ignore the error that `isDestroyAbortError` matches.
4. Call `onDestroyed(ref)` when the caller gave it.

`destroy` includes every partition that exists: routers, owners, and targets before their cutover. It removes
duplicates by `doName` over the whole traversal. A destroy that stops halfway can run again, because each parent
that remains still knows its children.

`FokosDB.destroy` calls `destroy()` on the coordinator client first, then on the partition client. A coordinator
drives each transaction in flight, so this order stops the drivers before the data goes.

#### 4.2.8 FokosDB on the client

| `FokosDB` method | After the change |
| --- | --- |
| `#putItem`, `#getItem`, `#deleteItem` | `this.#partitions.point("apiPutItem", { hashKey, sortKey }, req)`, and the same for the other two. |
| `#queryItems` | `this.#partitions.range("apiQueryItems", { hashKey, interval, descending }, req)` for each sub-query. |
| `#transactWriteItems` | The fast path applies when `resolveAll(keys).length === 1`. From M2, the coordinator request carries the table topology once, and the items carry no `partitionContext` (section 4.2.9). Before M2, `partitionContext` comes from `resolve(key)`. |
| `#readSnapshotFastPath` | The same `resolveAll` check, and the policy of section 4.2.5. |
| `#readTransaction` | Groups the items with `resolveAll`. Then it calls `send("txReadForTransaction", ctx, req, keys, { retry })` for each group, in both phases, with the policy of section 4.2.5. |
| coordinator call | `this.#coordinators.point("initiateWrite", tokenKey, req, { retry })`. `tokenKey` is the route key of the idempotency token. It replaces `tryWhile`. |
| `#destroy` | `this.#coordinators.destroy()`, then `this.#partitions.destroy()`. |

`FokosDB` takes the stub callbacks from `partitionStubByName` and `txCoordinatorStubByName`. `partitionInfoOf` and
`leafPartitionInfo` do not change, because they read `FokosPublicRouting`.

`resolveAll` groups by the root `doName`. That is the grouping of `singlePartitionTarget` and of
`#readTransaction` now. Both phases of a read transaction send the same groups.

#### 4.2.9 The coordinator request (M2)

**Now.** The Worker resolves every participant to a root:

- Each item of `initiateWrite` carries the full route context of the root of its hash key: `topology`,
  `rangeConfig`, the whole `FokosDbPolicy`, `partitionId`, and `doName`.
- The coordinator groups the participants by `op.partitionContext.doName`.
- It stores `tc_items.partition_do_name` for each item and `tc_participants.partition_context_json` for each
  participant. `txPrepare`, `txCommit`, `txCancel`, and recovery send to those stored roots.

**After M2:**

- **The request.** `initiateWrite` carries the table topology (`FokosTopology`) once. Each item carries its keys
  and its operation only. The range config and the policy come from the coordinator route context of the same
  request. `FokosDB` builds the coordinator client from the same table config, with the same `rangeConfig` and
  `policy`.
- **The resolution.** The coordinator makes a `FokosShardingClient` of the table topology, with the stub callback
  `partitionStubByName`. In `initiateWrite`, before the transition that writes `CREATED`, it groups the items
  with `resolveAll`. Every group is a root, as now.
- **The stored rows.** The coordinator writes `tc_items.partition_do_name` and
  `tc_participants.partition_context_json` from its own resolution. The columns do not change. `txPrepare`,
  `txCommit`, `txCancel`, recovery, and the migration of a coordinator split read the stored rows, as now. So the
  grouping stays the same for the whole protocol.
- **The sends.** The coordinator calls its participants with `send(op, storedCtx, req, keys, { retry })`, with
  the policies of section 4.2.5.
- **The idempotency hash.** `hashTransactionOperations` covers the keys and the operations, and not the contexts.
  The change does not affect it.

The reasons:

- The request is smaller. An item context holds the topology, the range config, the policy, and two identifiers.
  One root context with a jurisdiction and a location hint is 362 to 410 bytes in `v8.serialize`, for shard group
  names of 5 to 29 characters. At `MAX_ITEMS_PER_TX` (100) items, that is about 36 KB to 41 KB of repeated
  contexts in one request.
- The coordinator resolves its participants itself. When the route cache arrives, the coordinator connects its
  own cache in the same place (section 4.3.2).

The stale-transaction recovery of a partition calls a coordinator with `txCoordinatorStubForParticipant`. That
path does not change.

#### 4.2.10 A target before its cutover asks its source (M3)

**The lifecycle gate now.** A target in `awaiting_data` or `importing` runs `#whileImporting`:

- A `read_source` operation reads through its source with `fokosExecuteLocal`. While the source repartition is
  `queued` or `planned`, `resolveCallerSlice` in the source refuses it with `repartition_not_cut_over`.
- Every other operation answers `partition_migrating`.

A target in `awaiting_data` cannot know if its source has cut over, because the start notification can be lost.
Before the cutover, the source owns the slice and can serve every request. After the cutover, the target owns
the slice. Two hints can send a request to a target before its cutover:

- **A Bloom false positive** (problem 7). A hash partition sends a point or range request to the range root of a
  promotion that is held before its cutover. A read falls back through `readOnly && notCutOver` in
  `#fallbackAfterMiss`, or through `notCutOver` in `#forwardRangeVisit`. A write gets `partition_migrating` on
  every retry.
- **A caller-side route cache that outlives a destroy** (section 4.3.1). It can name a target of a new table with
  the same topology.

**The change.** In `awaiting_data`, the gate sends every operation that is not `local` to the source, through
`fokosExecuteLocal`. Before M3, only a `read_source` operation goes there. The source decides:

| Source repartition state | A `read_source` operation | Any other operation |
| --- | --- | --- |
| `queued`, `planned` | The source runs it. | The source runs it. |
| `cutover` or a later state | The source runs it, as now. | The source answers `repartition_cut_over`. |

- **The source runs the request as an owner.** It checks `owns(key)` for every route key. `owns` reads the
  topology and the route overrides only, and never the Bloom filter. So the source never forwards the request back
  to the target. The source then runs the admission, `beforeForward`, the local handler, and the signals, as for a
  request that it resolved locally. No `await` comes between the ownership check and the local handler, so the
  cutover of the source cannot come between the two. An `async` local handler keeps its own ownership checks, as
  for a direct request to the source now.
- **When a key is not owned, the source answers `repartition_cut_over`.** The slice of the target moves in one
  cutover. So every key of one request has the same owner.
- **The target takes `repartition_cut_over` as the start notification.** It moves to `importing`, as
  `fokosStartImport` does, and answers `partition_migrating`. A retry to the same entry is then correct, because
  the target owns the slice and imports it. A lost start notification now ends at the first write, and not at the
  next fallback alarm.
- **The routing.** The target lists itself as `read_through`, and the source lists itself as `executed`, as for a
  read through now.
- **`importing` does not change.** A `read_source` operation reads through the source, and every other operation
  answers `partition_migrating`. The source has cut over, so the target owns the slice.
- **`repartition_cut_over` is internal.** Only the target receives it, and it never leaves the gate.

After M3, no request path raises `repartition_not_cut_over`. M3 removes the two fallbacks that read it: the
`readOnly && notCutOver` condition of `#fallbackAfterMiss`, and the `notCutOver` condition of
`#forwardRangeVisit`. Their `range_partition_not_initialized` fallbacks stay. `servePage` and `acceptAck` keep
their `repartition_not_cut_over` for the migration protocol, which is not a request path. The mapping of
`repartition_not_cut_over` in `mapInternalErrorToPublic` stays, and the public codes do not change.

The cost: a request that reaches a target before its cutover pays one more RPC, from the target to the source.
Only a hint sends a request there.

#### 4.2.11 Performance

- The client sends the same RPCs as now, with the same retries.
- `resolve` and `resolveAll` run in memory, with no I/O.
- M2 removes the repeated route contexts from the coordinator request (section 4.2.9).
- M3 adds one RPC to a request that reaches a target before its cutover. It removes the retries that such a write
  pays now, which last until the cutover.

#### 4.2.12 Deployment and rollback

- M1 changes no RPC and no stored state. A rollback is a code revert.
- M1 adds `role` to `FokosStatusPage`. An older caller ignores the field.
- M1 changes the count of `forwardCount`: a failed forward without routing now counts 1. The public `meta` shows
  a larger `forwardCount` only for a request whose route had such a failure.
- M2 is a breaking change of the `initiateWrite` request. A coordinator of the new version does not accept the old
  request form. A Worker and its coordinators must run the same version. The package is not released, so no
  compatibility period applies.
- M2 does not change the coordinator tables. A transaction in flight keeps its stored root contexts.
- M3 changes what `fokosExecuteLocal` accepts. The target and the source are instances of one class, so they run
  the same version after a deploy. A rollback returns the target to `partition_migrating` for a write.

#### 4.2.13 Testing

- **M1 equivalence.** The `FokosDB` suites and `examples/http-api/test/demo2.test.ts` pass with no change to their
  expectations.
- **Retry policy.** A client without a policy does not retry. A call policy replaces the client policy.
  `shouldRetry` gets `nextAttempt` 2 on the first retry. A read-transaction phase fails twice with a transport
  error, then succeeds, and the read transaction returns the result.
- **walk.** `scope: "owners"` during a split yields no router and no target before its cutover. `walk` on a table
  that was never used creates no identity.
- **destroy.** The current destroy tests pass.
- **Call cost.**
  - A call fails with `partition_migrating` once, then succeeds at a root that serves locally: `clientRpcs` 2,
    `totalForwardCount` 0, `forwardCount` 0.
  - A call fails once after one forward, then succeeds after two forwards: `clientRpcs` 2, `totalForwardCount` 3.
  - A call that fails reports the same fields on its error.
- **Forward count without routing.** A router forwards to a hinted child without an identity, then falls back and
  succeeds. `forwardCount` includes the failed forward.
- **Coordinator request.** `initiateWrite` items carry no `partitionContext`. The coordinator stores the root
  contexts that the current code stores, and the transaction commits.
- **The target asks its source.**
  - A runtime test drives a Bloom false positive for a write into the range root of a promotion that a lock holds
    before its cutover. The source serves the write.
  - A test sends a group operation directly to a target in `awaiting_data` before the cutover. The operation
    applies at the source.
  - A target in `awaiting_data` after the cutover has a lost start notification. It answers the first write with
    `partition_migrating` and starts its import.
  - A source whose Bloom filter says that the key is promoted runs the request locally. It does not forward the
    request back to the target.
- **resolveAll.** Keys under two roots give two groups, and the positions map the results back to the input.
- **The bundle.** `pnpm build` passes the client bundle check and the size budget.

### 4.3 Future extensions

#### 4.3.1 The caller-side route cache

A later document adds a route cache to the client. The cache changes only `resolve`, `resolveRange`, and
`resolveAll`. It learns from the internal routing before `unwrap`. It depends on M3. That work must keep these
rules:

- **A cache hint never changes a result.** A miss, an old entry, a full cache, or no cache changes latency only.
  Partitions only split and never merge, so a partition that exists is a correct entry for the keys of its scope.
  After M3, that is also true for a target before its cutover.
- **The fallback test.** A `hash_partition_not_initialized` or `range_partition_not_initialized` error without
  routing comes from the entry itself. A partition without an identity attaches no routing, and it refuses before
  it forwards. Nothing applied anywhere, so a fallback is safe for every operation. The same code with routing
  comes from below the entry. The entry hint is then still valid, and the client must not remove it.
- **The fallback removes one hint, and never a whole cache or a whole key.** A bulk removal sends every request of
  the removed keys to the roots, and overloads them.
- **The client classifies an entry from its `partitionId`, not from a record of how it resolved the entry.** A hash
  `partitionId` gives the root index and the depth. A range `partitionId` gives the hash key and the interval. So
  a fallback works for an entry that the caller selected with `send`, after a restart, and for a context that a
  coordinator stored.
- **The range entry rule.** A range request enters only at a partition whose interval contains the whole
  requested interval. It never enters by one key. `docs/ideas/2026-09-20-query-entry-point-into-a-range-tree.md`
  records the incomplete pages that the other rule causes.
- **The cache learns only from nodes of its own shard group.** An error can carry the routing of another shard
  group. A host handler that calls a different group, and lets the error escape, keeps that routing, because
  `#guard` does not replace routing that is already there. A `partitionId` does not contain the shard group, but
  the node `ref.doName` does, and `FokosPublicRouting` keeps `ref`. The cache learns a node only when
  `PartitionIdHelper.doName(topology.shardGroup, Uint8Array.fromHex(node.ref.partitionId))` equals
  `node.ref.doName`. The routing needs no new field.
- **The representation allocates what it uses, and can evict.** `HashTopology` reserves its whole budget when it is
  created, 1 MiB by default, and it never frees a block. One instance for each root index does not fit the 128 MB
  memory limit of a Worker isolate
  ([Workers limits](https://developers.cloudflare.com/workers/platform/limits/)).
- **One default cache for each isolate.** tsdown puts a module that both entries import into a shared chunk. Now,
  `dist/client/index.js` and `dist/sharding/index.js` both import `dist/router-*.js`. So a module-scope default
  instance in `route-cache.ts` is one instance for the isolate. A build check asserts that `route-cache.ts` is in
  a shared chunk, as `check-client-bundle` asserts the client imports.
- **A stale hint can create an empty object.** After a destroy, a cache in another isolate still names the deep
  partitions of the old table. A send to one of them starts the object, and its constructor writes the schema.
  The object has no identity, so no `walk` and no `destroy` reaches it. Its storage stays until a later split
  creates the same name. Section 4.3.6 removes this cost.

#### 4.3.2 The coordinator reaches the participant leaves

With the route cache, the coordinator resolves each participant to its leaf, stores the deep context, and learns
from each participant envelope. The cache lives at module scope in the coordinator isolate, and its key includes
the table topology. A coordinator serves one table, so one hot table in a Worker cannot evict the entries of
another table. A later resolver can back the cache with the storage of the coordinator. It can also load an
encoded topology through an `import` method.

#### 4.3.3 One resolver for the runtime and the client

The runtime and the cached client apply the same rules: the resolution order, the learn rules, and the fallback.
A later change moves `#resolve`, `#learn`, and `#fallbackAfterMiss` into one pure class with a cache interface.
The runtime backs it with `FokosShardingStore`. The client backs it with its route cache.

#### 4.3.4 Other extensions

- **Batch item operations.** A batch operation is a `group` shape on the host. The caller uses `resolveAll`,
  builds one request for each group, and calls `send` for each group in parallel. The positions in each
  `FokosResolvedGroup` map the results back to the input. A retry policy on a batch is correct only when the batch
  operation is idempotent (section 4.2.4).
- **The call cost in the public `FokosDB` `meta`.** `FokosDB` can add `clientRpcs` and `totalForwardCount` to
  `meta`. This spec keeps `meta` as it is now.
- **A shared route spec.** For example, `defineRoutes<SearchOps>({ addDoc: { shape: "point", key: (r) => ... } })`.
  The host spreads it into `operations()`, and the client reads the key from it. Then `point("addDoc", req)` needs
  no explicit key. The spec can also mark an operation as idempotent, and give its default retry policy.
- **Range entries for many intervals.** A `resolveRanges` helper with the range entry rule, for a scan over many
  intervals.
- **Resume a range query deeper.** A query that resumes from a cursor still routes by its full interval. A host
  that narrows its request to the cursor can route by the narrower interval, and enter deeper.

#### 4.3.5 A refusal that proves that nothing applied

The runtime can mark an error that it raised before any handler ran on the path of the request. The destroy
fence, the identity check, the admission, and the lifecycle gate raise such errors. A router that ran a local part
clears the mark. The client can then retry a marked error for every operation without a policy. This spec does
not add the mark, because the caller policies cover every current call site.

#### 4.3.6 Schema migrations on the first real request

The runtime and the host run their schema migrations in the constructor. So a status call, or a send to a
partition that does not exist, writes a schema into an object that holds no data. The migrations can run on the
first request that gives or finds an identity. Then a cold object without an identity keeps no storage. This is
out of scope now.

## 5. Alternative options

- **The target refuses with `repartition_not_cut_over`, and the caller falls back to a shallower entry.** A
  caller cannot tell the refusal of its entry from the refusal of a partition below it. So it removes valid hints,
  and it can empty the cache of a key. A read with `whileMigrating: "throw"` also needs the same refusal. Only the
  target knows its source, so M3 lets the target ask its source.
- **A table generation in `FokosTopology`, against a cache that outlives a destroy.** The generation is part of
  every partition name, so an entry of the old table names no partition of the new table. It changes the
  identity of every partition, and each caller must get the new generation before it uses the new table. M3
  covers the same case, and it also fixes the Bloom path.
- **A default retry of `partition_migrating` in the client.** A `fail_fast` group and `initiateWrite` can raise
  that code after part of the request applied. A default retry applies a non-idempotent operation twice.
- **Callers group keys themselves for a batch.** Each caller needs the positions of its items in each group.
  `resolveAll` does it once.
- **Keep one `walk` with a fence option.** The fence stops the partition for good, and the two traversals visit
  in a different order. With two methods of two names, a caller cannot start a destroy by mistake.
- **Split a range request into visits on the caller.** The partitions already plan the frontier, and the merge
  logic belongs to the host. The caller selects one entry, and the partition plans from there.

## 6. Frequently asked questions

**Why is there no route cache in this document?** The cache needs a representation that fits a Worker isolate.
It also needs the fallback rules of section 4.3.1, and the fix of M3. This document builds the client that the
cache connects to, and it fixes the runtime first.

**Why does the client not retry by default?** A refusal does not prove that nothing applied (section 4.2.4). The
caller knows if its operation is idempotent. `FokosDB` and the demos pass their current rules as policies.

**Can the source forward the request of its target back to the target?** No. The source checks ownership with
`owns`, which never reads the Bloom filter. For a key that the source does not own, it answers
`repartition_cut_over`. The source never forwards the request.

**Does the client change the public `meta` of `FokosDB`?** Only the count of `forwardCount`: a failed forward
without routing now counts 1. `meta` comes from `FokosPublicRouting`, which the client returns as
`FokosRouter.unwrap` does now.

## 7. References

References:

- `docs/agent-plans/2026-09-19-fokos-sharding-runtime.md`
- `docs/agent-plans/2026-09-19-durable-object-jurisdictions.md`
- `docs/ideas/2026-09-20-query-entry-point-into-a-range-tree.md`
- `docs/ideas/range-partition-id-hierarchy-encoding.md`
- `docs/ideas/topology-propagation-via-piggyback.md`
- `packages/fokosdb/src/sharding/router.ts`
- `packages/fokosdb/src/sharding/runtime.ts`
- `packages/fokosdb/src/sharding/repartition-flow.ts`
- `packages/fokosdb/src/sharding/envelope.ts`
- `packages/fokosdb/src/sharding/hash-topology.ts`
- `packages/fokosdb/src/client/db.ts`
- `packages/fokosdb/src/server/do-transaction-coordinator.ts`
- `examples/http-api/src/demo2/`
- [Workers limits](https://developers.cloudflare.com/workers/platform/limits/)
