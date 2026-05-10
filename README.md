# fokosdb

FokosDB: A global strongly-consistent key-value database ontop of Cloudflare Durable Objects

## TODO

- Add topology keeper and encoding. Schema and versioning per change (split).
- Add partial topology caching in worker passed from response. Partition DOs also fetch periodically the topology (and store it in storage) and forward the request as far as they can instead of child partitions.
- Add range partitions (routing and splitting).
- Add heuristics for the split decision (cardinality of keys and frequency per key). See https://claude.ai/chat/50f7710a-2fcb-4022-895c-1a56904cc44e
- Make splitting and migration non-blocking. (Not needed when we get the Durable Objects forking/cloning API)
- Add WAE metrics per request, per split.
- Add large items through R2.
- Add conditional writes.
- Add distributed transactions (CASPaxos, Paxos Commit).

## FAQ

### Why not use the full 10GB of a Durable Object

### Why not one DO per hash key

### Why not a global range boundary mapping

Ala DynamoDB, hash the hash key and then create ranges across the entire table.

Pros:

- Easier to work even without a topology mapping cached.
- The topology mapping can be encoded in a much smaller format (LOUDS).
