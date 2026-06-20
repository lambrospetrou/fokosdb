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

No particular order.

- Expose an RPC/API to trigger a manual split.
- Optimize the range partition splitting (25% of total space instead of 50%, and see if there is a way to go straight to N partitions vs copying to root range).
- Update partial range topology within each partition to maintain also range boundaries to skip forwards in range partitions.
- Proper structured errors thrown to differentiate user vs server errors.
- Add FokosStd class with helper methods (e.g. paginator for queryItems).
- Enforce the expiration ttl for items.
- Batch item operations (non-transactions).
- Use an instance of the FokosDB (without transactions) as the durability ledger for Transaction Coordinators to allow stateless coordinators so that data partitions would be able to start recovery on any of them. It adds an extra hop though in the transaction flow.
- Allow check conditions and filter conditions on any attribute if the data is not bytes.
- Refactor do-partition tests from scratch now that everything is implemented and clean them up without internal knowledge.
- Add global eventual indexes (DynamoDB GSIs).
- Transactions across tables, think of a nice API due to how we handle PartitionContext.
- Add topology keeper and encoding. Schema and versioning per change (split).
- Think about backups and export in a consistent fashion.
- User provided code running inside the DO for N+1 operations. ONLY for library or self-hosted mode where the user controls the Durable Object class used, otherwise we would need Dynamic Workers and the `pipe()` operator.
- Add WAE metrics per request, per split.
- Add canonical logs per request in the service with an overridable requestId.
- Add optimization for single-partition transactions to not do 2PC.
- Add partial topology caching in worker passed from response. Partition DOs also fetch periodically the topology (and store it in storage) and forward the request as far as they can instead of only child partitions.
- Optimize the transaction timestamp/numbering to reduce conflicts at the millisecond level. Use the coordinator ID as tie breaker.
- Implement the timestamp ordering optimizations for transactions based on Section 4 of the ATC 2023 paper "Distributed Transactions at Scale in Amazon DynamoDB".
- Extend the split/migration flow to also allow writes while migration in-progress. Not needed once we use DO Snapshot API.
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
