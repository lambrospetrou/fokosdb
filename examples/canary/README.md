# canary

A continuous FokosDB demo. A cron trigger fans out every minute to one `WorkloadRunnerDO` per
(workload, placement, instance) triple. Each runner exercises the public `FokosDB` API against its
own table and writes Workers Analytics Engine points. The same phases are reachable over HTTP, so an
external runner can drive the same workloads on any placement, in parallel instances.

## Names

- Runner object: `wl:<workload>:<placement>:<instance>` — its own isolate per triple.
- FokosDB table: `canary-<workload>-<placement>-<instance>` — fixed, so a workload keeps its table across ticks.
- Placement is a Durable Object location hint (`wnam`, `enam`, `weur`, `eeur`, `apac`, `apac-ne`,
  `apac-se`, `oc`) or a jurisdiction (`eu`, `us`). A jurisdiction run also hints inside its region.

## Phases

Each runner object answers three RPCs, each taking `{workload, params?}`:

- `setup` — seeds the workload's table once; a `seeded:<workload>` flag in storage marks it done.
- `tick` — runs one metered run of the workload. Lazily seeds first when the flag is absent.
  Returns `{ok: false, reason: "busy"}` while another phase runs.
- `cleanup` — `db.destroy()` on the workload's table and clears the flag and the seen-partition set.

`params` can carry `iterations`, `concurrency` (≤6), `budgetMs` (≤45s). A run stops at the first of
the two bounds, so a slow region completes fewer iterations inside its budget.

## HTTP

- `GET /` — lists phases, workloads, placements.
- `POST /:phase` — body `{workload, placement, instance?, params?}`. `instance` defaults to `0`.
  Example: `POST /tick {"workload": "crud", "placement": "weur", "instance": "run-7"}`.
- `POST /sweep` — runs the same fan-out the cron does and returns the per-run results. The optional
  body `{placements?, workloads?, instance?}` narrows it: `POST /sweep {"workloads": ["crud"],
"placements": ["weur"]}`.
- Auth: when `DEMO_API_TOKEN` is set, requests need the `x-fokos-secret-token` header. The token is
  a required secret, never a var — see Deploy for how it is set.

## Cron

`* * * * *` → for every placement in `DEMO_PLACEMENTS` and every workload in `DEMO_CRON_WORKLOADS`,
calls `tick` on instance `DEMO_CRON_INSTANCE`. Runs are independent objects, so all run in parallel.

## Deploy

`DEMO_API_TOKEN` is a required secret, so the first deploy must upload it with the code — a plain
`wrangler deploy` fails the required-secret check, and `wrangler secret put` needs a Worker that
already exists.

```bash
# 1. Generate the secrets file (env format; .env* is gitignored)
echo "DEMO_API_TOKEN=$(openssl rand -hex 32)" > .env.production

# 2. First deploy: code and secrets go up together
pnpm deploy:secrets
```

Later code deploys need only `pnpm deploy` — the secret carries over from the previous version.
Rotate the token with `wrangler secret put DEMO_API_TOKEN`. For local dev, copy `.dev.vars.example`
to `.dev.vars` instead — `wrangler dev` reads it, `wrangler deploy` does not.

## Workloads

- `crud` — put → get → delete → get(miss) per iteration.
- `tx` — three lanes: multi-partition `transactWriteItems`, `transactGetItems`, `ordered_per_item` chain.
- `query` — `queryItems` pages over a seeded 8-hash-key dataset: between, begins_with, multi-hash-key, count, filter.
- `batch` — `batchWriteItems`(100) then `batchGetItems` of the same keys per iteration.
- `ttl` — put expiring in 5s, then polls `getItem` until the sweep deletes the row; reports the lag.

## Analytics Engine points

Dataset `fokos_canary`, bound as `ANALYTICS`. Every point carries
`blob1=pointType, blob2=workload, blob3=placement, blob4=instance, blob5=outcome,
blob6=actor_id, blob7=actor_name, blob8=colo, blob9=loc` — the runner object that publishes the
point and the region its isolate runs in — and the index `placement|workload|instance`. An
invocation publishes at most 250 points; overruns count in `tick`'s `dropped_n`.

Doubles follow one convention everywhere: `*_ms` durations first, then `*_n` counts, then `*_bytes`
sizes. In `iter` points `double1` is always `total_ms`, the wall-clock duration of one iteration —
for a workload that runs its ops in parallel it is the elapsed span, not a sum of the op durations —
so one chart can plot iteration latency across all workloads.

### Runner events, in publish order within a `tick`

- `runner` — a `tick` starts. No doubles; the point marks the run and carries the actor and
  location blobs.
- `seed` — the lazy setup seeded the table. Published only on the first `tick` of a table, or the
  first `tick` after a `cleanup`. `[seed_ms, items_n]`.
- `cold` — a partition actor name served this table for the first time. `[op_ms, database_bytes]`,
  blob10 = actor name. The runner persists the seen set and reports each actor once until `cleanup`
  resets it: this is the new-partition signal (split children, cold starts).
- `iter` — one per workload iteration. See below for the per-workload doubles.
- `tick` — the run summary. `[elapsed_ms, iterations_n, ops_n, errors_n, dropped_n]`.

### Other runner events

- `busy` — a `tick` arrived while a phase already ran; the run was skipped. No doubles.
- `cleanup` — the `cleanup` phase destroyed the table. `[elapsed_ms]`.

### `iter` doubles per workload

| workload | doubles                                                                                                            | blob10                           |
| -------- | ------------------------------------------------------------------------------------------------------------------ | -------------------------------- |
| `crud`   | `total_ms, put_ms, get_ms, delete_ms, miss_ms, forwards_n, database_bytes`                                         | —                                |
| `tx`     | `total_ms, partitions_n, forwards_n`                                                                               | lane: `write`, `read`, `ordered` |
| `query`  | `total_ms, pages_n, items_n, forwards_n, visits_n`                                                                 | query pattern                    |
| `batch`  | `total_ms, write_ms, read_ms, write_ok_n, write_unprocessed_n, read_found_n, read_not_found_n, read_unprocessed_n` | —                                |
| `ttl`    | `total_ms, lag_ms, polls_n`                                                                                        | —                                |

The `outcome` blob reports the first non-ok outcome of the iteration (a FokosDB error code, or
`ttl_pending` when the item outlived the budget). `transaction_cancelled` is a normal `tx` outcome.

## Layout

- `src/index.ts` — HTTP routes, the cron handler, the DO re-exports wrangler needs.
- `src/sweep.ts` — the tick fan-out the cron handler and `POST /sweep` share.
- `src/runner-do.ts` — `WorkloadRunnerDO`: the three phases, the busy guard, the metered `op` wrapper.
- `src/demo-dos.ts` — `DemoPartitionDO`, `DemoTransactionCoordinatorDO`, `DemoAutoAdminDO`: subclasses
  kept for `fokosConfig()` / `fokosRuntimeConfig()` overrides.
- `src/shared.ts` — placements, names, the `FokosDB` factory, the runner stub.
- `src/metrics.ts` — the point emitter, `meta` extraction, the seen-partition tracker, the colo fetch.
- `src/workloads/` — the `Workload` type, the registry, and one module per workload.

## TODO

- `hash-grow` workload: uniform writes that grow partitions toward the real 500 MB split cap.
- `range-grow` workload: one hot hash key to exercise promotion and range splits.
- Runner cold-start measure: caller-side `tick` latency that includes isolate boot.
- Migration telemetry: `partition_migrating` windows and `awaiting_data` durations per child partition.
- `walk()` census point: tree depth, router/owner counts, `importState` per table per tick.
- Coordinator split visibility: sustained unique-token transactions on a small coordinator cap.
- `gsi` workload: `captureChanges` + materialization DO, measure write-to-index lag.
- Per-workload cron schedules and per-workload `fokosConfig()`/`fokosRuntimeConfig()` overrides.
- Table retention policy: scheduled `cleanup` or table name rotation.
- Placements `fedramp`, `sam`, `afr`, `me` when the account allows them.
- vitest suite for the worker, like `examples/http-api` has.
