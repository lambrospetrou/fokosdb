# fokosdb

FokosDB: A global strongly-consistent key-value database ontop of Cloudflare Durable Objects

## TODO

No particular order.

- Implement a "walk partitions" helper RPC to get a live topology of the partitions.
- Cleanup pending transactions from the parent once split_completed.
- Refactor the ensurePartitionContext(pCtx) logic to check required immutable options and separate options that can change after first use.
- Optimize the transaction timestamp/numbering to reduce conflicts at the millisecond level.
- Use StaticShardedDO for the transaction coordinators.
- Implement the Jump Consistent Hashing instead/in addition of xxhash32.
- (perf) Allow each child partition to start migrating data before all partitions are initialized.
- Add topology keeper and encoding. Schema and versioning per change (split).
- Add partial topology caching in worker passed from response. Partition DOs also fetch periodically the topology (and store it in storage) and forward the request as far as they can instead of child partitions.
- Add WAE metrics per request, per split.
- Add global eventual indexes.
- Extend the split/migration flow to also allow writes while migration in-progress.
- Add range partitions (routing and splitting).
- Add heuristics for the split decision (cardinality of keys and frequency per key). See https://claude.ai/chat/50f7710a-2fcb-4022-895c-1a56904cc44e
- Support large items through R2.
- Support CASPaxosDO for the data partitions for multi-region availability.
- Extend distributed transactions to not depend on the TransactionCoordinator DOs with Paxos Commit and CAS Paxos.
- Migrate the splitting/migration to the Durable Objects forking/cloning API.

## FAQ

### Why not use the full 10GB of a Durable Object

### Why not one DO per hash key

### Why not a global range boundary mapping

Ala DynamoDB, hash the hash key and then create ranges across the entire table.

Pros:

- Easier to work even without a topology mapping cached.
- The topology mapping can be encoded in a much smaller format (LOUDS).
