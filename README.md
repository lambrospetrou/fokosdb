# fokosdb

FokosDB: A global strongly-consistent key-value database ontop of Cloudflare Durable Objects

## TODO

- Add range partitions (routing and splitting).
- Add partial topology caching in worker passed from response.
- Add topology encoding.
- Add topology keeper and propagation to workers (Workers KV).
- Make splitting and migration non-blocking. (Not needed when we get the Durable Objects forking/cloning API)

## FAQ

### Why not use the full 10GB of a Durable Object

### Why not one DO per hash key

### Why not a global range boundary mapping

Ala DynamoDB, hash the hash key and then create ranges across the entire table.

Pros:

- Easier to work even without a topology mapping cached.
- The topology mapping can be encoded in a much smaller format (LOUDS).
