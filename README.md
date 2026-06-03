# fokosdb

FokosDB: A global strongly-consistent key-value database ontop of Cloudflare Durable Objects

## TODO

No particular order.

- Implement query items with pagination.
- Add optimization for single-partition transactions to not do 2PC.
- Add topology keeper and encoding. Schema and versioning per change (split).
- Add partial topology caching in worker passed from response. Partition DOs also fetch periodically the topology (and store it in storage) and forward the request as far as they can instead of child partitions.
- Implement a "walk partitions" helper RPC to get a live topology of the partitions.
- Optimize the transaction timestamp/numbering to reduce conflicts at the millisecond level. Use the coordinator ID as tie breaker.
- Use an instance of the FokosDB (without transactions) as the durability ledger for Transaction Coordinators to allow stateless coordinators so that data partitions would be able to start recovery on any of them. It adds an extra hop though in the transaction flow.
- Implement the timestamp ordering optimizations for transactions based on Section 4 of the 2023 paper "Distributed Transactions at Scale in Amazon DynamoDB"
- Implement the Jump Consistent Hashing instead/in addition of xxhash32.
- Add WAE metrics per request, per split.
- Add canonical logs per request.
- Add global eventual indexes.
- Extend the split/migration flow to also allow writes while migration in-progress. Not needed once we use DO Snapshot API.
- Add heuristics for the split decision (cardinality of keys and frequency per key). See https://claude.ai/chat/50f7710a-2fcb-4022-895c-1a56904cc44e
- Support large items through R2.
- Support CASPaxosDO for the data partitions for multi-region availability. Use Paxos Commit and CAS Paxos for the topology keeper for higher availability (speed is no issue).
- Migrate the splitting/migration to the Durable Objects forking/cloning API.

## FAQ

### Why not use the full 10GB of a Durable Object

Cold starts, table operations.

### Why not one DO per hash key

Performance, running DOs respond faster, less cost for duration.

### Why not a global range boundary mapping

Ala DynamoDB, hash the hash key and then create ranges across the entire table.

Pros:

- Easier to work even without a topology mapping cached.
- The topology mapping can be encoded in a much smaller format (LOUDS).
