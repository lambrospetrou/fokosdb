---
name: profile-worker
description: Capture CPU and heap (allocation) profiles of a deployed fokosdb http-api worker or of one Durable Object partition, using synthetic get/put load and the Cloudflare profiling API. Use when the user asks to profile a deployed fokosdb worker, find hot functions, or measure allocations in production.
---

# Profile a deployed fokosdb worker

Produce pprof CPU and heap profiles of the deployed `examples/http-api` worker and of a Durable Object
partition that serves the traffic.

## Rules

- **Never write secrets to files.** The worker API token and the Cloudflare credentials travel in
  environment variables or in the credential stores that already exist. Do not put them in scripts,
  skill files, shell history you keep, or git.
- **Do not commit `.pprof` files.** A profile can hold string data from the isolate. Keep them in
  `/tmp` or in a git-ignored directory and share function names only.
- **Traffic must run before and during every capture.** The profiler attaches to a recently active,
  already-loaded isolate. It does not invoke the worker. Start the load first, capture second, and
  let the load run for the whole capture.
- **Use the deployed version UUID, not `latest`.** `latest` is the newest upload, which may not be
  deployed. Read the UUID with `cf workers versions list --worker-id <name>`.

## What you need

- `k6` and `go` (for `go tool pprof`). Both are repo tools: `tools/k6_profile.js` is the load script.
- The worker name and its `*.workers.dev` URL. Get both from `cf workers list` or from
  `wrangler.jsonc` (`name`) and the `subdomain.url` field of `cf workers list`.
- The account ID. Visible in `cf workers list` output, in any API error URL, or from
  `cf accounts list`.
- A valid worker API token. The http-api example authenticates with the `x-fokos-secret-token`
  header against the `FOKOS_API_TOKENS` secret. A copy usually lives in
  `examples/http-api/.env.production`. Pass it to k6 as `FOKOS_API_TOKEN`.
- Cloudflare API access for the profile call. Either `cf` CLI signed in, or a bearer token with
  **Workers Scripts Read** (or the Content Read-Only Workers role). When `cf` is signed in, its
  OAuth token sits in `~/.config/cloudflare/config/default.json` under `oauth_token` and works as a
  bearer token. Put it in `CF_TOKEN`.

## Which Durable Object namespace

The table's `ns` binding decides which namespace holds the partitions. Read `makeFokosDB` in
`examples/http-api/src/shared.ts`: it uses `CUSTOM_PARTITION_DO`, not `PARTITION_DO`. Map the binding
name to the `namespace_id` through the `references.durable_objects` array in `cf workers list`. A
wrong pair gives `10004 actor_id is not valid for namespace_id`.

## Step 1 — collect identifiers

```bash
cf workers list                                  # account id, worker name, subdomain URL, DO namespace ids
cf workers versions list --worker-id <name>      # deployed version UUID (the "id" of the deployed version)
```

## Step 2 — generate load

`tools/k6_profile.js` sends 70% `getItem` / 30% `putItem` at a fixed arrival rate.

```bash
k6 run \
  -e BASE_URL="https://<worker>.<subdomain>.workers.dev" \
  -e FOKOS_API_TOKEN="<worker api token>" \
  -e TABLE="profile-load" -e RPS=200 -e DURATION=300s \
  tools/k6_profile.js
```

- **`ROOTS=1` is the default and the important part.** `partitionOptions.rootTreesN: 1` sends every
  request to one root partition, so one DO does all the work. Profiles of one hot isolate are much
  denser than the same traffic spread over many roots.
- **Use a fresh `TABLE` per `rootTreesN` value.** The topology is fixed at first write; a different
  `rootTreesN` on an existing table breaks routing.
- Tune `RPS`, `DURATION`, `DATA_LEN`, `HKS`, `SKS` through `-e`. The scenario warms up for ~30 s
  before the full rate so the profiler finds a hot isolate.
- The run logs `serving actor: <actor id> (<actor name>)` on its first response — that is the
  `actor_id` for a Durable Object capture. `http_req_failed` stays at 0 when it works.
- Keep the load running until every capture is done.

Get the full actor ID of the partition (for a DO capture) from any response `meta`:

```bash
curl -sS -X POST "$BASE_URL/api/rpc/$TABLE/getItem" \
  -H "x-fokos-secret-token: $FOKOS_API_TOKEN" -H "Content-Type: application/json" \
  -d '{"hashKey":"hk-0","sortKey":"sk-0","partitionOptions":{"rootTreesN":1}}' \
  | python3 -c "import json,sys;print(json.load(sys.stdin)['meta']['partitions'][0]['servedByActorId'])"
```

`servedByActorName` confirms which partition it is (for example `fokos.p.<table>~h.0`).

## Step 3 — capture profiles

Prefer the REST API. The `cf` CLI (beta) can return `10393 No recent executions` even while traffic
runs; the API works. Duration is 1000–50000 ms. Space captures ~60 s apart; on HTTP 429 honor
`Retry-After`.

```bash
ACCOUNT_ID="<account id>"
WORKER="fokos-svc-alpha"
VERSION="<deployed version uuid>"
CF_TOKEN="<bearer token>"

# Worker-level CPU profile, 50 s (the maximum; more samples).
curl -sS -X POST \
  "https://api.cloudflare.com/client/v4/accounts/$ACCOUNT_ID/workers/workers/$WORKER/versions/$VERSION/profile" \
  -H "Authorization: Bearer $CF_TOKEN" -H "Content-Type: application/json" \
  --data '{"duration_ms":50000,"profile_type":"cpu"}' -o worker-cpu.pprof

# One Durable Object partition: add namespace_id and actor_id.
curl -sS -X POST \
  "https://api.cloudflare.com/client/v4/accounts/$ACCOUNT_ID/workers/workers/$WORKER/versions/$VERSION/profile" \
  -H "Authorization: Bearer $CF_TOKEN" -H "Content-Type: application/json" \
  --data '{"duration_ms":50000,"profile_type":"cpu","namespace_id":"<ns id>","actor_id":"<actor id>"}' \
  -o do-cpu.pprof
```

- `"profile_type":"heap"` gives the allocation profile.
- Success returns `application/vnd.google.protobuf` (raw pprof, sometimes gzip). A JSON body is an
  error — read it before analysis.
- The `cf` CLI equivalent is
  `cf workers versions profile <version uuid> --worker-id <name> --duration-ms 50000 --profile-type cpu > out.pprof`
  plus `--namespace-id` and `--actor-id` for a DO.
- A **worker-level capture attaches to any recently active isolate** of the worker, not necessarily
  the isolate that runs the Hono routes. Under DO-heavy load it usually lands on the DO isolate, so
  the "worker" profile can show DO internals. To see the fetch isolate, keep DO load low and hit
  endpoints that do work in the worker itself.

## Step 4 — analyze

```bash
go tool pprof -top -nodecount=30 do-cpu.pprof    # flat/cum table
go tool pprof -http=:8080 do-cpu.pprof           # flamegraph in the browser
```

How to read the result:

- **CPU counts JS on-CPU time only.** RPC plumbing and the SQLite call run outside JS, so most
  request latency is invisible to the sampler. A few percent of wall time is normal for an I/O-bound
  worker. Low total samples mean low JS work, not a broken capture — concentrate the load on one DO
  (`--roots 1`) to raise density.
- **Heap is `alloc_space`: allocations, not retained memory.** It samples one stack per ~512 kB
  allocated. It does not show what stays live; a leak needs a different investigation.
- Frames show `<unknown>` when the deployed version has no source maps
  (`upload_source_maps` in `wrangler.jsonc`). Turn it on and redeploy for readable stacks.
- Compare like with like: `flat` is time/allocation inside the function itself, `cum` includes its
  callees. Per-request work that runs for every operation (dispatch, topology checks, the split
  evaluation hook, serialization) is what a hot partition makes visible.

## Failure modes

- `10393 No recent executions were found` — the version had no traffic in an active isolate. Confirm
  the load is running (k6 prints a rising request rate), confirm the version UUID is the deployed
  one, wait ~30 s and retry. If `cf` keeps failing under confirmed load, use the REST API.
- `10004 actor_id is not valid for namespace_id` — wrong namespace. Re-check which binding `ns` the
  table uses and map it again through `cf workers list`.
- `429 Too Many Requests` — captures are rate-limited. Wait for `Retry-After` (~60 s between
  captures is safe).
- Empty or tiny heap profile — allocation sampling needs volume. Extend `DURATION`, raise `RPS`, or
  raise `DATA_LEN`.
- `401` from the worker API — bad `FOKOS_API_TOKEN`. `403` from the Cloudflare API — the bearer
  token lacks Workers Scripts Read.
