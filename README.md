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

- Implement queryItems with pagination similar to the PartiQL execution though that allows multiple hash keys too.
- Batch item operations (non-transactions).
- Allow check conditions and filter conditions on any attribute if the data is not bytes.
- Update partial range topology within each partition to maintain also range boundaries.
- Refactor do-partition tests from scratch now that everything is implemented and clean them up without internal knowledge.
- Transactions across tables, think of a nice API due to how we handle PartitionContext.
- Add topology keeper and encoding. Schema and versioning per change (split).
- Think about backups and export in a consistent fashion.
- User provided code running inside the DO for N+1 operations. ONLY for library or self-hosted mode where the user controls the Durable Object class used, otherwise we would need Dynamic Workers and the `pipe()` operator.
- Add WAE metrics per request, per split.
- Add canonical logs per request in the service with an overridable requestId.
- Add optimization for single-partition transactions to not do 2PC.
- Add partial topology caching in worker passed from response. Partition DOs also fetch periodically the topology (and store it in storage) and forward the request as far as they can instead of only child partitions.
- Optimize the transaction timestamp/numbering to reduce conflicts at the millisecond level. Use the coordinator ID as tie breaker.
- Use an instance of the FokosDB (without transactions) as the durability ledger for Transaction Coordinators to allow stateless coordinators so that data partitions would be able to start recovery on any of them. It adds an extra hop though in the transaction flow.
- Implement the timestamp ordering optimizations for transactions based on Section 4 of the ATC 2023 paper "Distributed Transactions at Scale in Amazon DynamoDB".
- Implement Jump Consistent Hashing instead/in addition of xxhash32.
- Add global eventual indexes.
- Extend the split/migration flow to also allow writes while migration in-progress. Not needed once we use DO Snapshot API.
- Add heuristics for the split decision (cardinality of keys and frequency per key). See https://claude.ai/chat/50f7710a-2fcb-4022-895c-1a56904cc44e
- Support large items through R2.
- Support CASPaxosDO for the data partitions for multi-region availability. Use Paxos Commit and CAS Paxos for the topology keeper for higher availability (speed is no issue).
- Migrate the splitting/migration to the Durable Objects forking/cloning API.

## Benchmarks

_TODO_

## Contributing

This project is still in prototype and design mode, so I don't really want new features to be contributed by external folks, yet.

You can submit issues for bugs if you find something, or start a discussion if you have ideas, questions, or something else to say.
