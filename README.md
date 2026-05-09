# fokosdb

FokosDB: A global strongly-consistent key-value database ontop of Cloudflare Durable Objects

## FAQ

### Why not one DO per hash key

### Why not a global range boundary mapping

Ala DynamoDB, hash the hash key and then create ranges across the entire table.
