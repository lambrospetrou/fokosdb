# Stress write tests — runs sequentially within this file.
#
# Run with:
#   hurl --variables-file test/hurl/vars.env test/hurl/stress-writes.hurl
#
# Repeat the whole file N times in parallel:
#   hurl --test --repeat 10 --jobs 10 \
#        --variables-file test/hurl/vars.env test/hurl/stress-writes.hurl

GET {{base_url}}/static/text_10k.txt
HTTP 200
[Captures]
txt_10k: body

GET {{base_url}}/static/text_100k.txt
HTTP 200
[Captures]
txt_100k: body

# ── putItem: fully random hashKey + sortKey (100x) ──────────────────────────
# Each iteration writes to a unique key — exercises fan-out across partitions.

POST {{base_url}}/api/rpc/stress_001/putItem
Content-Type: application/json
[Options]
repeat: 100
{
  "partitionOptions": {
    "rootTreesN": 2,
    "hashSplitConditions": {
      "splitN": 4,
      "maxSizeMb": 2
    }
  },
  "hashKey": "stress#{{newUuid}}",
  "sortKey": "{{newUuid}}",
  "data": "payload-{{newUuid}}"
}
HTTP 200

# ── putItem: fully random hashKey + sortKey with 10K text (100x) ──────────────────────────
# Each iteration writes to a unique key — exercises fan-out across partitions.

POST {{base_url}}/api/rpc/stress_001/putItem
Content-Type: application/json
[Options]
repeat: 100
{
  "partitionOptions": {
    "rootTreesN": 2,
    "hashSplitConditions": {
      "splitN": 4,
      "maxSizeMb": 2
    }
  },
  "hashKey": "stress#{{newUuid}}",
  "sortKey": "{{newUuid}}",
  "data": "payload-{{newUuid}}-{{txt_10k}}"
}
HTTP 200

# ── putItem: fully random hashKey + sortKey with 100K text (100x) ──────────────────────────
# Each iteration writes to a unique key — exercises fan-out across partitions.

POST {{base_url}}/api/rpc/stress_001/putItem
Content-Type: application/json
[Options]
repeat: 100
# Retries are needed to avoid 500s during partition splitting.
retry: 3
{
  "partitionOptions": {
    "rootTreesN": 2,
    "hashSplitConditions": {
      "splitN": 4,
      "maxSizeMb": 2
    }
  },
  "hashKey": "stress#{{newUuid}}",
  "sortKey": "{{newUuid}}",
  "data": "payload-{{newUuid}}-{{txt_100k}}"
}
HTTP 200

# ── transactWriteItems: 10 random hashKeys in one transaction ───────────────

POST {{base_url}}/api/rpc/stress_001/transactWriteItems
Content-Type: application/json
{
  "partitionOptions": {
    "rootTreesN": 2,
    "hashSplitConditions": {
      "splitN": 4,
      "maxSizeMb": 2
    }
  },
  "operations": [
    { "hashKey": "batch#{{newUuid}}", "operation": "put", "data": "batch-value-0" },
    { "hashKey": "batch#{{newUuid}}", "operation": "put", "data": "batch-value-1" },
    { "hashKey": "batch#{{newUuid}}", "operation": "put", "data": "batch-value-2" },
    { "hashKey": "batch#{{newUuid}}", "operation": "put", "data": "batch-value-3" },
    { "hashKey": "batch#{{newUuid}}", "operation": "put", "data": "batch-value-4" },
    { "hashKey": "batch#{{newUuid}}", "operation": "put", "data": "batch-value-5" },
    { "hashKey": "batch#{{newUuid}}", "operation": "put", "data": "batch-value-6" },
    { "hashKey": "batch#{{newUuid}}", "operation": "put", "data": "batch-value-7" },
    { "hashKey": "batch#{{newUuid}}", "operation": "put", "data": "batch-value-8" },
    { "hashKey": "batch#{{newUuid}}", "operation": "put", "data": "batch-value-9" }
  ]
}
HTTP 200

# ── putItem: fixed hashKey (current datetime) + random sortKey (100x) ───────
# All iterations target the same hash-key bucket — exercises a hot partition.
# hashKey is the RFC 3339 UTC datetime at the moment each request fires.
#
#POST {{base_url}}/api/rpc/stress_001/putItem
#Content-Type: application/json
#[Options]
#repeat: 100
#{
#  "partitionOptions": {
#    "rootTreesN": 2,
#    "hashSplitConditions": {
#      "splitN": 4,
#      "maxSizeMb": 2
#    }
#  },
#  "hashKey": "ts#{{newDate}}",
#  "sortKey": "{{newUuid}}",
#  "data": "ts-payload-{{newUuid}}"
#}
#HTTP 200
