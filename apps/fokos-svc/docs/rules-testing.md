# Testing rules

Date: 2026-08-30
Status: **active; these rules describe the tests as they are today**

How this service is tested, and the rules a new test must follow. Each rule states what to do, why, and where the code
does it.

The platform rules are in `docs/rules-cloudflare.md`. The database invariants a test must protect are in `AGENTS.md`.

## Table of contents

- [The commands](#the-commands)
- [Tests run in the real runtime](#tests-run-in-the-real-runtime)
- [Every suite owns its own namespace](#every-suite-owns-its-own-namespace)
- [Drive Durable Objects through the test APIs](#drive-durable-objects-through-the-test-apis)
- [A test must be deterministic](#a-test-must-be-deterministic)
- [What a test must cover](#what-a-test-must-cover)
- [Some invariants are checked by grep](#some-invariants-are-checked-by-grep)
- [HTTP and load tests run against a deployed worker](#http-and-load-tests-run-against-a-deployed-worker)

---

## The commands

| Command                    | What it runs                                                               |
| -------------------------- | -------------------------------------------------------------------------- |
| `npm run test`             | `check` + `check:keys` + `vitest run --printConsoleTrace`                  |
| `npm run check`            | `tsc --noEmit` for both projects, then `prettier --check .`                |
| `npm run check:keys`       | The grep backstop for the key invariants (`tools/check-key-invariants.sh`) |
| `npm run test:hurl`        | The HTTP suites against the local dev worker                               |
| `npm run test:hurl:remote` | The HTTP suites against the deployed worker                                |
| `npm run k6:remote`        | The load script against the deployed worker                                |

Rules:

- **Run `npm run test` in a subagent.** Its output is long and fills the context.
- **Use `vitest -t <name>` for a repeated check** of one test while you work. Do not loop the whole suite.
- A change is finished when `npm run test` passes. A passing focused run is not a passing suite.

## Tests run in the real runtime

Unit and integration tests run in workerd through `@cloudflare/vitest-pool-workers`, configured from the root
`wrangler.jsonc` (`vitest.config.ts`). A test that passes in Node proves nothing about the runtime this code ships to.

`test/worker-entry.ts` is the library test entry. Durable Object classes must be exported from the Worker `main` module,
so this file re-exports every class the tests bind. It also exports `CustomPartitionDO extends PartitionDO`, and
`db.test.ts` runs its whole suite over that binding as well, so the subclass surface stays covered.

Add a class to `worker-entry.ts` and to the `migrations` list in `wrangler.jsonc` in the same change. A binding that
names a class the entry does not export fails at startup.

## Every suite owns its own namespace

Durable Objects are global to the test worker, so two tests that use one name share storage. Each suite therefore builds
its keys under a `crypto.randomUUID()` prefix:

```ts
const prefix = `test.${crypto.randomUUID()}`;
const base = PartitionContextCreator.create({ ns: "PARTITION_DO", tableName: prefix, ... });
```

Rules:

- Never hard-code a table name, a partition name, or a Durable Object name in a test.
- Build the stub through the same router the product code uses, not by naming an object directly. `makeStub` in
  `src/lib/do-partition.test.ts` is the pattern.
- Set the topology for determinism, not for realism. One root partition and a split factor of 2 make a split reachable
  and its outcome predictable.

## Drive Durable Objects through the test APIs

- `runInDurableObject(stub, (instance, state) => ...)` from `cloudflare:test` reaches the instance and its
  `DurableObjectState`. Use it to assert internal state and to call a private path a test must reach.
- `runDurableObjectAlarm(stub)` runs a scheduled alarm now. **Never wait for a real alarm to fire.** A test that sleeps
  is slow and flaky.
- `env` comes from `cloudflare:workers` for the product path and from `cloudflare:test` where the test helpers need it.
- Wrap a namespace in a `Proxy` when a test must observe which object a call reaches (`test/transactions.test.ts`).

## A test must be deterministic

- **No wall-clock waits.** Inject the clock where staleness or skew is under test (`ClockFn` in
  `src/lib/partition/transaction-participant.ts`) and run alarms explicitly.
- **No network.** The test configuration sets `FOKOS_SHOULD_FETCH_COLO_INFO: false` through the Miniflare bindings, so
  no test depends on an outside call. Keep it that way.
- **Reach a threshold with a budget, not with data volume.** Override the budget through a `__testing__` field —
  `__testing__migrationBatchLimitBytes` is the pattern — instead of writing megabytes to trigger a split or a batch
  boundary.
- `__testing__` fields exist for tests alone. No product logic may read one.
- A test hook that must run at an exact point in a flow is an awaited callback, such as
  `__testing__beforeMigrationComplete`, not a timing guess.

## What a test must cover

- **The failure path, not only the success path.** A retry, a lost reply, a concurrent writer, and a crash between two
  steps each need a test. These are the paths that carry the defects.
- **Each limit, and one value above it.** A limit with no test above the boundary is not enforced.
- **Idempotency.** Call `prepare`, `commit`, `cancel`, and every recovery path twice, and assert the second call changes
  nothing.
- **The public boundary shape.** Assert that a public result carries no internal field —
  `expect(meta).not.toHaveProperty("_internal")` — because structural typing accepts an extra property and the type
  checker will not catch it.
- **The interaction with the other state machines.** A new operation needs a test that runs it during a migration and
  during a split.

## Some invariants are checked by grep

`tools/check-key-invariants.sh` fails the build when raw string-comparison primitives (`charCodeAt`, `codePointAt`,
`localeCompare`) appear outside `key-codec.ts`, or when the NUL-joiner key pattern appears anywhere.

Add a check there when a rule is mechanical and a reviewer can miss it. A grep backstop costs nothing per run and does
not depend on a test reaching the line.

## HTTP and load tests run against a deployed worker

- The hurl suites in `test/hurl/` exercise the example HTTP API end to end. They take their variables from an env file,
  so the same suite runs against dev (`npm run test:hurl`) and against the deployed worker (`npm run test:hurl:remote`).
- `tools/k6_basic.js` is the load script. Run it against a deployment, never against a local dev worker, when the
  question is throughput or per-object capacity.
- These suites need a running target. They are not part of `npm run test` and do not gate a change on their own.
