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

FokosDB is not yet extracted into a properly structured package and is now mixed into a test Worker that exposes any number of databases through a REST API.

This is intentional to allow quick iteration during initial development.
Once there is a stable version ready, I will properly refactor the directory structure and publish an NPM library with only the actual FokosDB library.

## TODO

The following are in no particular order.

The code has `FIXME` and `TODO` items as well, so check those periodically too.

### Performance and Reliability

- Optimize the range partition splitting to go straight to N partitions vs copying to root range.
- Use the partial range topology within each partition to speed up transactions as well.
- Add topology keeper and encoding. Schema and versioning per change (split).
- Add partial topology caching in worker passed from response. Partition DOs also fetch periodically the topology (and store it in storage) and forward the request as far as they can instead of only child partitions.
- Garbage collect the items table after splits and hash key promotions.
- Create RpcTargets for the partition DOs and LRU cache them in the Worker to skip the getActor calls and go directly to the partition DOs.
- Add optimization for single-partition transactions to not do 2PC.
- Use an instance of the FokosDB (without transactions) as the durability ledger for Transaction Coordinators to allow stateless coordinators so that data partitions would be able to start recovery on any of them. It adds an extra hop though in the transaction flow. Or put enough info in the transaction sent to each partition so that they can communicate with the involved partitions to learn the outcome of the transaction.
- Circuit breaker for overloaded DOs, keep an LRU-cache in the isolate memory of a Worker and reject reqs to a DO for 1-2s.
- Optimize the transaction timestamp/numbering to reduce conflicts at the millisecond level. Use the coordinator ID as tie breaker.
- Implement the timestamp ordering optimizations for transactions based on Section 4 of the ATC 2023 paper "Distributed Transactions at Scale in Amazon DynamoDB".
- Extend the split/migration flow to also allow writes while migration in-progress (probably will need some kind of logical replication of writes after the migration started `_fokos_replication_log`). Not needed once we use DO Snapshot API.

### Features

- Cleanup the public API, both for `do-partition.ts` and `db.ts`.
- Decide how to handle location hints (example: root partitions use location hint but child partitions do not to stay close to the root and make the forwarding and migrations faster).
- Proper structured errors thrown to differentiate user vs server errors.
- Check for background alarms runaway errors due to errors, for example: `✘ [ERROR] Uncaught Error: fokos: initFromSplit called with conflicting options. child: ad5552a31e5a5114e6c86c803e1b4b246f682f228be84e94591af0d193355059 vs ad5552a31e5a5114e6c86c803e1b4b246f682f228be84e94591af0d193355059, parent: undefined vs 12b4100173770e9309970f0603f1e4fa4b0fa58877fb760afd31a29eef73691e, splitType: undefined vs hash Error`
- Add a healthcheck of each partition DO to a provider Workers KV namespace (do name -> partition context, split status, migrations status), since this could be better than a central DO for the state of the partitions, and could also be used by the PartitionTopologyKeeperDO.
- Expose an RPC/API to trigger a manual split.
- Enforce the expiration ttl for items.
- Add FokosStd class with helper methods (e.g. paginator for queryItems).
- Add jurisdictions support.
- Batch item operations (non-transactions).
- Allow check conditions and filter conditions on any attribute if the data is JSON.
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
rm -rf ./wrangler && npm run dev

# terminal 2
npm run test:hurl
```

## Contributing

This project is still work in progress and does breaking changes, so I don't really want new features to be contributed by external folks, yet.

You can submit issues for bugs if you find something, or start a discussion if you have ideas, questions, or something else to say.
