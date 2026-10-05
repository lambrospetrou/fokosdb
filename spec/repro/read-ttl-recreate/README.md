# Bug: TTL expiry let a read return values that never coexisted

A two-partition read must return values from one consistent state.
At commit `1b5aace`, the read could return `old-a` and `new-b`.
A TTL sweep deleted A before a transaction wrote `new-b`. Those values never existed together.
The read must reject that result with `read_conflict`.

This directory contains a separate Worker. It uses the built `fokosdb` client and server.
Its test-only `ReproPartitionDO` delays reads but does not change their results.
The Worker uses the real TTL timer and commits a two-partition write.
It changes no production code.

## Why the code at 1b5aace missed the write

At commit `1b5aace`, the TTL sweep deleted A without increasing `delete_revision`.
The next insert recreated A with version 1, the same version as `old-a`.
Both read passes saw A as present with version 1.
The client compared presence, version, and `delete_revision`, not the payload.
It could accept `old-a` from before the sweep and `new-b` from after the write.

Commit `edcbb56` changes deletion metadata and version allocation.
On current `main`, this one controlled schedule returns `read_conflict`.
That result does not prove that all read and TTL schedules are safe.
The [PlusCal model](../../interactions/ReadTtlRecreate.tla) describes only the rules at `1b5aace`.
The regression test passes on current `main` for the controlled schedule.

## What the model checks

The model has two keys, one per partition. Each key starts with an old value.
The Clock makes A's TTL due. The Sweeper deletes A when it holds no lock.
The Writer locks both keys. It commits the new value of A before B.
The Reader samples both keys twice. A pending write makes the read fail.
Otherwise, the Reader checks presence, version, and `delete_revision` between the passes.

The ghost variable `history` records each physical state, including the state between the two local commits.
Only the invariant `NoImpossibleRead` reads `history`.
It asks whether an accepted result matches one of those states.
TLC finds an allowed step order where no state matches the accepted result.
The model omits the timer scheduler, RPC delivery, migration, and other transactions.
It cannot show how often real requests follow that order.

## The controlled order

1. The test writes `old-a` with a TTL and writes `old-b` without a TTL.
2. Partition A reads `old-a`, then holds its answer. Partition B waits before its read.
3. The test releases A and waits for the real TTL timer to delete A.
4. A two-partition transaction commits `new-a` and `new-b`. The test checks both results.
5. The test releases B. The client must reject the read with `read_conflict`.

At `1b5aace`, the read returns `old-a` and `new-b`.
Those values never coexisted: the TTL sweep removed A before the transaction wrote B.
The test forces the order. It does not show that normal traffic produces the order without controls.

## Check the current code

From the repository root:

```sh
pnpm install --frozen-lockfile
pnpm build
pnpm --filter fokosdb exec vitest run test/transactions/tx-read-ttl-recreate.test.ts
pnpm --filter @fokosdb-repro/read-ttl-recreate check
```

To check the hosted runtime, deploy a temporary Worker with `cf`:

```sh
cd spec/repro/read-ttl-recreate
cf deploy --dry-run --profile personal
cf deploy --profile personal
node check.mjs <deployed-worker-url>
cf workers delete fokosdb-repro-read-ttl-recreate --profile personal
```

`cf deploy` prints the URL. Replace `personal` with your own profile name.
The runner exits with status 0 for `read_conflict` and status 1 for the impossible pair.
It exits with status 2 if it cannot check the result. Status 2 is not a pass.
Run the delete command even if the runner exits with status 1 or 2.
The `/run` endpoint is public. Delete the Worker as soon as the check ends.

The Worker uses a new table for each run and calls `destroy()` when the run ends.
Its name is `fokosdb-repro-read-ttl-recreate`.
A deploy with that name replaces a Worker of the same name in the chosen profile.
Confirm that the name does not belong to another service before you deploy or delete it.

## Repeat the failure at 1b5aace

Use a separate worktree for the code at `1b5aace`.
Start from a checkout of this PR branch, at a commit that contains this directory:

```sh
repro_commit=$(git rev-parse HEAD)
git worktree add --detach ../FokosDB-at-1b5aace 1b5aace
cd ../FokosDB-at-1b5aace
git restore --source "$repro_commit" -- .prettierignore pnpm-workspace.yaml spec/repro/read-ttl-recreate
pnpm install --frozen-lockfile=false
pnpm build
pnpm --filter @fokosdb-repro/read-ttl-recreate check
```

Run `cf dev` from `spec/repro/read-ttl-recreate` to check the old source locally.
In a second terminal, run `node spec/repro/read-ttl-recreate/check.mjs http://localhost:5173` from the worktree root.
The command exits with status 1 and reports `old-a` with `new-b`.

For a hosted check, run `cf deploy --profile personal` from `spec/repro/read-ttl-recreate` in that worktree.
Then run `node check.mjs <deployed-worker-url>` from the same directory.
Delete the temporary Worker with the `cf workers delete` command shown above.
Delete it after each hosted check, even when the runner reports the bug.

## Check the model

Get `tla2tools.jar` from the [TLA+ releases](https://github.com/tlaplus/tlaplus/releases).
Translate a temporary copy so the source file stays free of generated TLA+:

```sh
export TLA2TOOLS_JAR=/absolute/path/to/tla2tools.jar
model=$(mktemp -d)
cp spec/interactions/ReadTtlRecreate.{tla,cfg} "$model/"
(cd "$model" && java -cp "$TLA2TOOLS_JAR" pcal.trans ReadTtlRecreate.tla && \
  java -cp "$TLA2TOOLS_JAR" tlc2.TLC -workers 1 -config ReadTtlRecreate.cfg ReadTtlRecreate.tla)
```

TLC reports `Invariant NoImpossibleRead is violated.` for the rules at `1b5aace`.
The model does not describe current `main`.
The TLC trace does not prove that a real request reaches this order without the test gates.
