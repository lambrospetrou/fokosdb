# Property-based suites

The suites in this directory use [fast-check](https://fast-check.dev/). Each suite says at its top what it covers. `harness.ts` holds the model, the commands and the shared tables, and `query-harness.ts` holds the model of `queryItems`.

Run every command on this page from `packages/fokosdb`.

## Run the suites

```sh
pnpm vitest run test/property-based/
```

Each property has a small default run count, so that the full test suite stays fast. To search deeper, set `FOKOS_PROPERTY_RUNS`. It applies to every property of the run:

```sh
FOKOS_PROPERTY_RUNS=500 pnpm vitest run test/property-based/transactions-concurrent.test.ts
```

## Replay a failure

A failure report has the usual fast-check report, and after it, the commands that replay the failure. For example:

```text
Replay this failure from packages/fokosdb. README.md in test/property-based says more.

The counterexample alone:
  FOKOS_PROPERTY_SEED=280724494 FOKOS_PROPERTY_PATH='257' FOKOS_PROPERTY_REPLAY_PATH='EE:B' pnpm vitest run test/property-based/transactions-concurrent.test.ts -t 'batches of concurrent transactions agree with an in-memory map and leave no lock behind'
Every run of the seed up to the failure (run 258), and the shrink again:
  FOKOS_PROPERTY_SEED=280724494 FOKOS_PROPERTY_RUNS=258 pnpm vitest run test/property-based/transactions-concurrent.test.ts -t 'batches of concurrent transactions agree with an in-memory map and leave no lock behind'
```

Copy a command and run it. You do not change a suite to replay it.

| Variable                     | Value                              | Effect                                                                                                          |
| ---------------------------- | ---------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `FOKOS_PROPERTY_SEED`        | `seed` of the report               | Every property of the run uses this seed.                                                                       |
| `FOKOS_PROPERTY_PATH`        | `path` of the report               | The property runs only the counterexample at this path, and does not shrink it. It needs `FOKOS_PROPERTY_SEED`. |
| `FOKOS_PROPERTY_REPLAY_PATH` | `replayPath` in the counterexample | `fc.commands` builds the same shrunk sequence of commands. A sequence that did not shrink runs without it.      |
| `FOKOS_PROPERTY_RUNS`        | a positive integer                 | The run count of every property of the run.                                                                     |

The seed applies to every property of the run, so keep the `-t` of the command. It selects one test.

A `seed`, a `path` or a `replayPath` that a suite gives in its code wins over the variable. fast-check merges the settings key by key, so give the seed and the path in the same place: a seed in the code with a path from the shell replays nothing.

`vitest.config.ts` copies these variables into the test modules when it builds them, because a test inside the Workers runtime cannot read the shell environment. `replay-setup.ts` applies the seed and the path to fast-check, and adds the commands to a failure.

## When a replay passes

The seed sets the input of a run. It does not set the timing of the Durable Objects, and it does not set the state that earlier runs left in the table. Thus a replay can pass for a failure that is real.

- **A race.** Many failures need two events at the same time, for example a partition that splits while a transaction prepares on it. The replay sends the same commands, but the split can start at a different time.
- **A shared table.** The split suites share one seeded table across all the runs of a property, and every run writes to it. The first command runs the counterexample alone and skips the earlier runs, so the table is in a different state. The second command runs every input of the seed again, in the same order, up to the failure. It is slower, but it gets nearer to the state of the failed run.

When the first command passes, use the second command. If it also passes, run it more than one time, or run the full suite with a higher `FOKOS_PROPERTY_RUNS`. A failure that a replay never shows again is usually a race: read the report and its cause, and find the two events that must overlap.
