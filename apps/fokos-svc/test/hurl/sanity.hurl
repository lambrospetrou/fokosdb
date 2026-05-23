# Run with:
#   hurl --variables-file test/hurl/vars.env test/hurl/sanity.hurl
#
# Target a remote deployment:
#   hurl --variable base_url=https://your-worker.your-subdomain.workers.dev \
#        --variables-file test/hurl/vars.env test/hurl/sanity.hurl

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
jsonpath "$.data" == "Alice"
jsonpath "$.dataEncoding" == "utf8"

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
jsonpath "$.data" == "profile-data"
jsonpath "$.dataEncoding" == "utf8"

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
