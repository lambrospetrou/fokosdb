# Run with:
#   npm run test:hurl:run -- test/hurl/sanity.hurl
#
# Target the remote deployment (see test/hurl/vars_remote.env for configuration):
#   npm run test:hurl:remote -- test/hurl/sanity.hurl

# ── Hello ──────────────────────────────────────────────────────────────────────

GET {{base_url}}/api/hello/world
HTTP 200
[Asserts]
jsonpath "$.message" == "Hello, world!"

GET {{base_url}}/api/hello/fokos
HTTP 200
[Asserts]
jsonpath "$.message" == "Hello, fokos!"

# ── putItem ────────────────────────────────────────────────────────────────────

POST {{base_url}}/api/rpc/{{database_name}}/putItem
Content-Type: application/json
{
  "hashKey": "user#001",
  "data": "Alice"
}
HTTP 200

# ── getItem (found) ────────────────────────────────────────────────────────────

POST {{base_url}}/api/rpc/{{database_name}}/getItem
Content-Type: application/json
{
  "hashKey": "user#001"
}
HTTP 200
[Asserts]
jsonpath "$.found" == true
jsonpath "$.item.data" == "Alice"
jsonpath "$.item.dataEncoding" == "utf8"

# ── getItem (not found) ────────────────────────────────────────────────────────

POST {{base_url}}/api/rpc/{{database_name}}/getItem
Content-Type: application/json
{
  "hashKey": "nonexistent#key"
}
HTTP 200
[Asserts]
jsonpath "$.found" == false

# ── putItem with sortKey ───────────────────────────────────────────────────────

POST {{base_url}}/api/rpc/{{database_name}}/putItem
Content-Type: application/json
{
  "hashKey": "user#001",
  "sortKey": "profile",
  "data": "profile-data"
}
HTTP 200

POST {{base_url}}/api/rpc/{{database_name}}/getItem
Content-Type: application/json
{
  "hashKey": "user#001",
  "sortKey": "profile"
}
HTTP 200
[Asserts]
jsonpath "$.found" == true
jsonpath "$.item.data" == "profile-data"
jsonpath "$.item.dataEncoding" == "utf8"

# ── putItem with ttlSeconds ────────────────────────────────────────────────────

POST {{base_url}}/api/rpc/{{database_name}}/putItem
Content-Type: application/json
{
  "hashKey": "ttl#item",
  "data": "expires-soon",
  "ttlSeconds": 3600
}
HTTP 200

# ── putItem with conditions ────────────────────────────────────────────────────

POST {{base_url}}/api/rpc/{{database_name}}/putItem
Content-Type: application/json
{
  "hashKey": "cond#item",
  "data": "v1",
  "conditions": [{ "type": "item_not_exists" }]
}
HTTP 200

# overwrite fails if item_not_exists condition is applied again
POST {{base_url}}/api/rpc/{{database_name}}/putItem
Content-Type: application/json
{
  "hashKey": "cond#item",
  "data": "v2",
  "conditions": [{ "type": "item_not_exists" }]
}
HTTP *
[Asserts]
status >= 400
status < 600

# ── deleteItem ─────────────────────────────────────────────────────────────────

POST {{base_url}}/api/rpc/{{database_name}}/deleteItem
Content-Type: application/json
{
  "hashKey": "user#001"
}
HTTP 200

POST {{base_url}}/api/rpc/{{database_name}}/getItem
Content-Type: application/json
{
  "hashKey": "user#001"
}
HTTP 200
[Asserts]
jsonpath "$.found" == false

# item with sortKey is unaffected by deleting the bare hashKey
POST {{base_url}}/api/rpc/{{database_name}}/getItem
Content-Type: application/json
{
  "hashKey": "user#001",
  "sortKey": "profile"
}
HTTP 200
[Asserts]
jsonpath "$.found" == true

# ── deleteItem with conditions ─────────────────────────────────────────────────

POST {{base_url}}/api/rpc/{{database_name}}/deleteItem
Content-Type: application/json
{
  "hashKey": "cond#item",
  "conditions": [{ "type": "item_exists" }]
}
HTTP 200

# ── queryItems: two hash keys, different scanIndexForward ─────────────────────

POST {{base_url}}/api/rpc/{{database_name}}/transactWriteItems
Content-Type: application/json
{
  "operations": [
    { "hashKey": "qi#asc", "sortKey": "s1", "operation": "put", "data": "asc-1" },
    { "hashKey": "qi#asc", "sortKey": "s2", "operation": "put", "data": "asc-2" },
    { "hashKey": "qi#asc", "sortKey": "s3", "operation": "put", "data": "asc-3" },
    { "hashKey": "qi#desc", "sortKey": "s1", "operation": "put", "data": "desc-1" },
    { "hashKey": "qi#desc", "sortKey": "s2", "operation": "put", "data": "desc-2" },
    { "hashKey": "qi#desc", "sortKey": "s3", "operation": "put", "data": "desc-3" }
  ]
}
HTTP 200

# query both hash keys: qi#asc ascending (default), qi#desc descending
POST {{base_url}}/api/rpc/{{database_name}}/queryItems
Content-Type: application/json
{
  "queries": [
    { "hashKey": "qi#asc", "scanIndexForward": true },
    { "hashKey": "qi#desc", "scanIndexForward": false }
  ]
}
HTTP 200
[Asserts]
jsonpath "$.count" == 6
jsonpath "$.items[0].hashKey" == "qi#asc"
jsonpath "$.items[0].sortKey" == "s1"
jsonpath "$.items[0].data" == "asc-1"
jsonpath "$.items[1].hashKey" == "qi#asc"
jsonpath "$.items[1].sortKey" == "s2"
jsonpath "$.items[1].data" == "asc-2"
jsonpath "$.items[2].hashKey" == "qi#asc"
jsonpath "$.items[2].sortKey" == "s3"
jsonpath "$.items[2].data" == "asc-3"
jsonpath "$.items[3].hashKey" == "qi#desc"
jsonpath "$.items[3].sortKey" == "s3"
jsonpath "$.items[3].data" == "desc-3"
jsonpath "$.items[4].hashKey" == "qi#desc"
jsonpath "$.items[4].sortKey" == "s2"
jsonpath "$.items[4].data" == "desc-2"
jsonpath "$.items[5].hashKey" == "qi#desc"
jsonpath "$.items[5].sortKey" == "s1"
jsonpath "$.items[5].data" == "desc-1"

# ── transactWriteItems ─────────────────────────────────────────────────────────

POST {{base_url}}/api/rpc/{{database_name}}/transactWriteItems
Content-Type: application/json
{
  "operations": [
    { "hashKey": "tx#item1", "operation": "put", "data": "value1" },
    { "hashKey": "tx#item2", "operation": "put", "data": "value2" },
    { "hashKey": "tx#item3", "sortKey": "sk", "operation": "put", "data": "value3" }
  ]
}
HTTP 200

# ── transactGetItems ───────────────────────────────────────────────────────────

POST {{base_url}}/api/rpc/{{database_name}}/transactGetItems
Content-Type: application/json
{
  "items": [
    { "hashKey": "tx#item1" },
    { "hashKey": "tx#item2" },
    { "hashKey": "tx#item3", "sortKey": "sk" }
  ]
}
HTTP 200
[Asserts]
jsonpath "$.outcome" == "committed"
jsonpath "$.items" count == 3
jsonpath "$.items[0].found" == true
jsonpath "$.items[0].data" == "value1"
jsonpath "$.items[0].dataEncoding" == "utf8"
jsonpath "$.items[1].found" == true
jsonpath "$.items[1].data" == "value2"
jsonpath "$.items[2].found" == true
jsonpath "$.items[2].data" == "value3"

# transactGetItems for a missing key reports not found within a committed result
POST {{base_url}}/api/rpc/{{database_name}}/transactGetItems
Content-Type: application/json
{
  "items": [
    { "hashKey": "tx#item1" },
    { "hashKey": "does-not-exist#999" }
  ]
}
HTTP 200
[Asserts]
jsonpath "$.outcome" == "committed"
jsonpath "$.items[0].found" == true
jsonpath "$.items[1].found" == false

# transactWriteItems with check operation
POST {{base_url}}/api/rpc/{{database_name}}/transactWriteItems
Content-Type: application/json
{
  "operations": [
    { "hashKey": "tx#item1", "operation": "check", "conditions": [{ "type": "item_exists" }] },
    { "hashKey": "tx#item2", "operation": "delete" }
  ]
}
HTTP 200

# ── putItem: same hashKey, multiple sortKeys ──────────────────────────────────

POST {{base_url}}/api/rpc/{{database_name}}/putItem
Content-Type: application/json
{
  "hashKey": "range#001",
  "sortKey": "sk#a",
  "data": "range-value-a"
}
HTTP 200

POST {{base_url}}/api/rpc/{{database_name}}/putItem
Content-Type: application/json
{
  "hashKey": "range#001",
  "sortKey": "sk#b",
  "data": "range-value-b"
}
HTTP 200

# each sort key is independently retrievable
POST {{base_url}}/api/rpc/{{database_name}}/getItem
Content-Type: application/json
{
  "hashKey": "range#001",
  "sortKey": "sk#a"
}
HTTP 200
[Asserts]
jsonpath "$.found" == true
jsonpath "$.item.data" == "range-value-a"

POST {{base_url}}/api/rpc/{{database_name}}/getItem
Content-Type: application/json
{
  "hashKey": "range#001",
  "sortKey": "sk#b"
}
HTTP 200
[Asserts]
jsonpath "$.found" == true
jsonpath "$.item.data" == "range-value-b"

# bare hashKey lookup misses when only sort-keyed entries exist
POST {{base_url}}/api/rpc/{{database_name}}/getItem
Content-Type: application/json
{
  "hashKey": "range#001"
}
HTTP 200
[Asserts]
jsonpath "$.found" == false

# ── Error: unknown rpcAction ───────────────────────────────────────────────────

POST {{base_url}}/api/rpc/{{database_name}}/unknownAction
Content-Type: application/json
{}
HTTP 404

# ── Error: invalid JSON body ───────────────────────────────────────────────────

POST {{base_url}}/api/rpc/{{database_name}}/putItem
Content-Type: application/json
```
invalid-json
```
HTTP 400

# ── Error: validation failure (missing hashKey) ────────────────────────────────

POST {{base_url}}/api/rpc/{{database_name}}/putItem
Content-Type: application/json
{
  "sortKey": "only-sort-no-hash"
}
HTTP 400
[Asserts]
jsonpath "$.error" == "Validation failed"

# ── Error: validation failure (missing data in putItem) ────────────────────────

POST {{base_url}}/api/rpc/{{database_name}}/putItem
Content-Type: application/json
{
  "hashKey": "some-key"
}
HTTP 400
[Asserts]
jsonpath "$.error" == "Validation failed"

# ── Error: both ttl fields provided ───────────────────────────────────────────

POST {{base_url}}/api/rpc/{{database_name}}/putItem
Content-Type: application/json
{
  "hashKey": "ttl-conflict",
  "data": "test",
  "ttlSeconds": 60,
  "ttlEpochUTCSeconds": 9999999999
}
HTTP 400
[Asserts]
jsonpath "$.error" == "Validation failed"
