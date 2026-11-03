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
- Auth: when `DEMO_API_TOKEN` is set, requests need the `x-fokos-secret-token` header.

## Cron

`* * * * *` → for every placement in `DEMO_PLACEMENTS` and every workload in `DEMO_CRON_WORKLOADS`,
calls `tick` on instance `DEMO_CRON_INSTANCE`. Runs are independent objects, so all run in parallel.

## Workloads

- `crud` — put → get → delete → get(miss) per iteration.
- `tx` — three lanes: multi-partition `transactWriteItems`, `transactGetItems`, `ordered_per_item` chain.
- `query` — `queryItems` pages over a seeded 8-hash-key dataset: between, begins_with, multi-hash-key, count, filter.
- `batch` — `batchWriteItems`(100) then `batchGetItems` of the same keys per iteration.
- `ttl` — put expiring in 5s, then polls `getItem` until the sweep deletes the row; reports the lag.

## Analytics Engine points

Dataset `fokos_canary`, bound as `ANALYTICS`. Blobs always start
`blob1=pointType, blob2=workload, blob3=placement, blob4=instance, blob5=outcome`; the index is
`placement|workload|instance`.

- `runner` — `[boot_age_ms]`; blob6=colo, blob7=loc.
- `seed` — `[seeded_items, seed_ms]`.
- `iter` — per iteration; doubles per workload:
  - `crud`: `[put_ms, get_ms, delete_ms, get_miss_ms, database_size, forwards]`
  - `tx`: `[ms, partitions, forwards]`, blob6 = lane (`write` | `read` | `ordered`)
  - `query`: `[pages, items, ms_total, forwards, partition_visits]`, blob6 = pattern
  - `batch`: `[write_ms, read_ms, write_ok, write_unprocessed, read_found, read_not_found, read_unprocessed]`
  - `ttl`: `[lag_ms, polls]` (`lag_ms` = −1 when the item outlived the budget; outcome `ttl_pending`)
- `cold` — first call served by a partition name: `[op_ms, database_size]`, blob6 = actor name.
- `tick` — run summary: `[iterations_done, elapsed_ms, ops, errors, dropped_points]`.
- `busy` — a `tick` that found a phase already running.
- `cleanup` — `[elapsed_ms]`.

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
- Migration telemetry: `partition_migrating` windows and `awaiting_data` durations per child partition.
- `walk()` census point: tree depth, router/owner counts, `importState` per table per tick.
- Coordinator split visibility: sustained unique-token transactions on a small coordinator cap.
- `gsi` workload: `captureChanges` + materialization DO, measure write-to-index lag.
- Per-workload cron schedules and per-workload `fokosConfig()`/`fokosRuntimeConfig()` overrides.
- Table retention policy: scheduled `cleanup` or table name rotation.
- Placements `fedramp`, `sam`, `afr`, `me` when the account allows them.
- vitest suite for the worker, like `examples/http-api` has.
- `DEMO_API_TOKEN` as a secret instead of a plain var before a real deployment.
