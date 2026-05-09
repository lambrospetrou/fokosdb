# fokosdb

FokosDB: A global strongly-consistent key-value database ontop of Cloudflare Durable Objects

## TODO

- Add range partitions (routing and splitting).
- Make splitting and migration non-blocking.
- Add partial topology caching in worker passed from response.
- Add topology encoding.
- Add topology keeper and propagation to workers (Workers KV).

## FAQ

### Why not one DO per hash key

### Why not a global range boundary mapping

Ala DynamoDB, hash the hash key and then create ranges across the entire table.
