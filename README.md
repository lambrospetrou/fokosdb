# fokosdb

FokosDB: A strongly consistent bottomless storage database ontop of Cloudflare Durable Objects

Read the article introducing FokosDB and explaining the reasoning behind its architecture: <https://www.lambrospetrou.com/articles/fokosdb>

> [!CAUTION]
> **Do NOT use this in production, yet.**
>
> I am still doing breaking changes, and will continue doing so for a few weeks, so do not store any data you will need again until a version is published that I consider stable enough.
>
> **You have been warned!**

## Project structure

This is a pnpm workspace.

```
packages/
  fokosdb/            the published library
    src/client/       FokosDB and the routing surface  -> fokosdb/client
    src/server/       the Durable Object classes       -> fokosdb/server
    src/shared/       code both entries use, inlined into each at build time
examples/
  http-api/           deployable Worker exposing tables over REST
```

`fokosdb` publishes exactly two subpath exports, `fokosdb/client` and `fokosdb/server`. There is no
bare `fokosdb` import. See [`packages/fokosdb/README.md`](./packages/fokosdb/README.md) for the API
and for the Durable Object re-export that wrangler requires.

## Commands

This repo uses pnpm. If you do not have it, install it with `npm install -g pnpm`, or enable the version Node ships with by running `corepack enable pnpm`.

Run these from the repo root.

| Command           | Purpose                                                          |
| ----------------- | ---------------------------------------------------------------- |
| `pnpm install`    | Install every workspace package                                  |
| `pnpm build`      | Build the library into `packages/fokosdb/dist`                   |
| `pnpm test`       | Build, then run the library and example suites in real `workerd` |
| `pnpm dev`        | Build, then boot the example Workers with wrangler               |
| `pnpm check`      | Typecheck every package and check formatting                     |
| `pnpm fmt`        | Format the repo                                                  |
| `pnpm lint:pkg`   | `publint` on the packaged library                                |
| `pnpm cf-typegen` | Regenerate every `worker-configuration.d.ts`                     |

The examples import the library's built `dist/`, not its sources, so `pnpm build` has to run before
their tests. The `test` and `dev` scripts already do this.

## TODO

The following are in no particular order.

The code has `FIXME` and `TODO` items as well, so check those periodically too.

### Performance and Reliability

- Use an instance of the FokosDB (without transactions) as the durability ledger for Transaction Coordinators to allow stateless coordinators so that data partitions would be able to start recovery on any of them. It adds an extra hop though in the transaction flow. Or put enough info in the transaction sent to each partition so that they can communicate with the involved partitions to learn the outcome of the transaction.
- Garbage collect the transactions data from the tx coordinators. Only delete a transaction that is already COMMITTED or CANCELLED, since partitions read a missing transaction as cancelled.
- Garbage collect the `range_hierarchy` table of each partition. It is written on every forwarded request and never pruned.
- Garbage collect the items table after splits and hash key promotions.
- Optimize the range partition splitting to go straight to N partitions vs copying to root range.
- Use the partial range topology within each partition to speed up transactions as well.
- Add topology keeper and encoding. Schema and versioning per change (split).
- Add partial topology caching in worker passed from response. Partition DOs also fetch periodically the topology (and store it in storage) and forward the request as far as they can instead of only child partitions.
- Create RpcTargets for the partition DOs and LRU cache them in the Worker to skip the getActor calls and go directly to the partition DOs.
- Circuit breaker for overloaded DOs, keep an LRU-cache in the isolate memory of a Worker and reject reqs to a DO for 1-2s.
- Optimize the transaction timestamp/numbering to reduce conflicts at the millisecond level. Use the transaction ID as tie breaker, since it is stable no matter which coordinator stamps or resumes the transaction.
- Count item data sizes in UTF-8 bytes, to match the `octet_length` the store uses. The current UTF-16 count under-counts non-ASCII data by up to 3x, so the item and transaction size caps admit more than they intend.
- Implement the timestamp ordering optimizations for transactions based on Section 4 of the ATC 2023 paper "Distributed Transactions at Scale in Amazon DynamoDB".
- Allow transactional reads during a migration by falling back to the parent partition, as `getItem` and `queryItems` already do.
- Extend the split/migration flow to also allow writes while migration in-progress (probably will need some kind of logical replication of writes after the migration started `_fokos_replication_log`). Not needed once we use DO Snapshot API.

### Features

- Proper structured errors thrown to differentiate user vs server errors. Pick ONE failure model: a failed condition throws in `putItem` but is a returned `cancelled` value in `transactWriteItems`. Report one cancellation reason per operation, in request order.
- Expose an RPC/API to trigger a manual split.
- Enforce the expiration ttl for items. `ttlSeconds` is currently dropped without error, and a transactional put clears an existing expiry because `TransactionItem` carries no ttl.
- Cleanup the public API, both for `do-partition.ts` and `db.ts`. One item envelope for `getItem`, `queryItems` and `transactGetItems`, so a client can write a single item decoder.
- Return the same `meta` (operation metrics and partition info) from `transactWriteItems` and `transactGetItems` as every other operation returns.
- Decide how to handle location hints for root partitions and transaction coordinators. Child partitions should stay close to the root for faster forwarding and migrations. `transactGetItems` runs its two-phase driver in the caller Worker; add an option to run it through a coordinator placed close to the partitions when the Worker is far from them.
- Add jurisdictions support.
- Allow custom split conditions in user-provided function of the partition DO class, and also for custom item selection per child partition.
- Implement projections, filter expression, and update expressions as described in `docs/agent-plans/2026-08-29-typed-expression-engine-spec.md`. Condition expressions are fully implemented.
- Check for background alarms runaway errors due to errors, for example: `✘ [ERROR] Uncaught Error: fokos: initFromSplit called with conflicting options. child: ad5552a31e5a5114e6c86c803e1b4b246f682f228be84e94591af0d193355059 vs ad5552a31e5a5114e6c86c803e1b4b246f682f228be84e94591af0d193355059, parent: undefined vs 12b4100173770e9309970f0603f1e4fa4b0fa58877fb760afd31a29eef73691e, splitType: undefined vs hash Error`
- Add a healthcheck of each partition DO to a provider Workers KV namespace (do name -> partition context, split status, migrations status), since this could be better than a central DO for the state of the partitions, and could also be used by the PartitionTopologyKeeperDO.
- Add FokosStd class with helper methods (e.g. paginator for queryItems).
- Batch item operations (non-transactions).
- Refactor do-partition tests from scratch now that everything is implemented and clean them up without internal knowledge.
- Add global eventual indexes (DynamoDB GSIs).
- Consider adding reference tables, small tables replicated in all partitions. Useful on their own, and also with anything we do for server-side procedures.
- Transactions across tables, think of a nice API due to how we handle PartitionContext.
- Think about backups and export in a consistent fashion.
- User provided code running inside the DO for N+1 operations. ONLY for library or self-hosted mode where the user controls the Durable Object class used, otherwise we would need Dynamic Workers and the `pipe()` operator.
- Add WAE metrics per request, per split.
- Add canonical logs per request in the service with an overridable requestId.
- Add heuristics for the split decision (cardinality of keys and frequency per key). See https://claude.ai/chat/50f7710a-2fcb-4022-895c-1a56904cc44e
- Support large items through R2.
- Support CASPaxosDO for the data partitions for multi-region availability. Use Paxos Commit and CAS Paxos for the topology keeper for higher availability (speed is no issue).
- Migrate the splitting/migration to the Durable Objects forking/cloning API.

## Benchmarks

_TODO_

## Development

```sh
npm test
```

### Test with Hurl

```sh
# terminal 1
rm -rf ./examples/http-api/.wrangler && pnpm dev

# terminal 2
pnpm --filter "@fokosdb-example/http-api" test:hurl
```

## Contributing

This project is still work in progress and does breaking changes, so I don't really want new features to be contributed by external folks, yet.

You can submit issues for bugs if you find something, or start a discussion if you have ideas, questions, or something else to say.
