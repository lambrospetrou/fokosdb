# Idea — Local simulation of the Durable Object snapshot API

**State:** Draft
**Date:** 2026-11-28
**Author:** Lambros Petrou

**Status:** A test-only fake is built under `packages/fokosdb/test/snapshot/`. No production code uses the snapshot API
yet. The code paths in this document are relative to `packages/fokosdb/`.

## Table of contents

- [1. Overview and context](#1-overview-and-context)
- [2. Goals](#2-goals)
- [3. Platform facts](#3-platform-facts)
- [4. The options that we tried](#4-the-options-that-we-tried)
- [5. What to adopt](#5-what-to-adopt)
- [6. How the tests use it](#6-how-the-tests-use-it)
- [7. Where the fake differs from production](#7-where-the-fake-differs-from-production)
- [8. Measurements](#8-measurements)
- [9. Next steps](#9-next-steps)
- [10. References](#10-references)

## 1. Overview and context

Two planned features use the Durable Object snapshot API:

- **Split and promotion by snapshot.** A child of a split starts from a snapshot of its parent at the latest storage
  position. This replaces the page-by-page import of the repartition flow
  ([the repartition RFC](../agent-plans/2026-09-17-unified-repartition-flow.md)).
- **Backup and restore** ([the backup RFC](2026-11-17-consistent-backup-and-restore.md)).

The API does not operate in local development. Our tests and the planned deterministic simulation testing
([the DST RFC](../agent-plans/2026-11-16-protocol-aware-deterministic-simulation-testing.md)) run locally. Thus we need
a local simulation of the API. The production code must call the same API locally and on Cloudflare, with no second
code path.

This document records each option that we tried, the result, and the options to adopt. It covers the repartition
step first. Backup and restore use the same mechanism.

## 2. Goals

1. The production code calls `ctx.storage.snapshot()` and `ctx.storage.onNextSessionRestore()` locally, as on
   Cloudflare. No flag and no branch in `src/`.
2. A restore replaces all the storage of the target DO before its constructor runs, as on Cloudflare.
3. A snapshot handle travels over RPC from the source DO to the target DO.
4. The simulation works for DOs of 1 MiB, 100 MiB and 1 GiB.
5. `kvData` writes its keys after the restore, and it replaces a copied key with the same name.
6. The DST simulator controls when a restore lands.

## 3. Platform facts

Each fact below comes from workerd 1.20261123.1 (the version of the lockfile) under `@cloudflare/vitest-plugin`, from
its source code or from a scratch test.

| Fact | Source |
| --- | --- |
| `ctx.storage.snapshot({ bookmark? })` returns an opaque `DurableObjectSnapshot`. It can travel only over RPC: "DurableObjectSnapshots cannot be cloned or stored". | `src/workerd/api/snapshot.h` |
| `ctx.storage.onNextSessionRestore(snapshotOrBookmark, { kvData? })` schedules a restore for the next start. It returns an undo bookmark. The caller must restart the DO. | `src/workerd/api/actor-state.h` |
| `kvData` holds at most 20 keys and 3,072 bytes of keys and V8-serialized values. The restore writes it before the constructor runs. It works only with a snapshot target. | `actor-state.c++` |
| The compatibility flag `durable_object_snapshots` (or `experimental`) shows both methods. A facet cannot call them. | `compatibility-date.capnp`, `actor-state.c++` |
| With the flag, both methods exist locally, and both throw. `snapshot()` throws "This Durable Object's storage back-end does not implement snapshots". A restore from a bookmark throws "... does not implement point-in-time recovery". | Scratch test |
| The local back-end `ActorSqlite` does not override `captureBookmarkSnapshot` or `onNextSessionRestore`. | `src/workerd/io/actor-cache.h`, `actor-sqlite.c++` |
| `Object.defineProperty(ctx.storage, "snapshot", ...)` on the real storage object works. The DO code then calls the replacement. | Scratch test |
| `super()` of a DO class refuses a `Proxy` of `ctx`. | The DST RFC, section 4.2.2 |
| SQL cannot read the KV table `_cf_KV` (`SQLITE_AUTH`). A copy must read the KV keys through `ctx.storage.kv`. | Scratch test |
| Inside workerd, `node:fs` writes to `/tmp` work. A later call of the same DO reads the file. Another DO does not find it, and the file is not on the disk of the host. A write outside `/tmp` fails. | Scratch test |
| Miniflare accepts a Node function as a service binding. A DO can call it with `fetch`, also with a body of 5 MB. | Scratch test |
| One SQL statement with a recursive CTE and `randomblob` fills 1 GiB of a DO in about 3.6 s. A read of 1 GiB of blobs into memory takes about 1.2 s. | Scratch test |

## 4. The options that we tried

| # | Option | Result | Decision |
| --- | --- | --- | --- |
| O1 | Use the real API locally with the flag | The methods exist, but they throw. | Not possible locally. Add the flag anyway, so that the generated types show the real API. |
| O2 | Give the DO a `Proxy` of `ctx` with the fake methods | `super()` refuses a `Proxy` of `ctx`. | Rejected. |
| O3 | Replace the two methods on the real `ctx.storage` in a test subclass | Works. The production constructor sees only the restored storage. | **Adopt.** |
| O4 | Read the KV keys through SQL | `SQLITE_AUTH` on `_cf_KV`. | Rejected. The copy uses `ctx.storage.kv.list()`. |
| O5 | Keep the copies in `/tmp` with `node:fs` inside workerd | Each DO has its own `/tmp`, in the memory of workerd. The target DO cannot read the copy of the source. | Rejected. |
| O6 | Keep the copies in module state (backend `memory`) | Works. All the DOs of a test file share one module instance. The fastest backend. | **Adopt as the default.** |
| O7 | Keep the copies in a test DO with SQLite (backend `do`) | Works. Needs a page format, because one row holds at most 2 MB. The slowest backend. | Keep. It is the only backend that needs no Node process, so it can also work under `wrangler dev`. We did not test that. |
| O8 | Keep the copies on the disk of the host through a Node service binding (backend `host`) | Works. Each copy is a directory of JSON pages. A person can read it after the run. | **Adopt for investigation and for DST replays.** |
| O9 | Keep one JSON string for each copy | A 1 GiB copy is about 1.4 G characters, and V8 limits a string to about 512 M characters. We did not run it. | Rejected. The text form is a sequence of pages of at most 1 M characters. |
| O10 | Apply the restore asynchronously in the constructor, with `blockConcurrencyWhile` | Not tried. The production constructor reads the storage synchronously, so it would see the old storage. | Rejected. The restore applies synchronously before `super()`. The copy must be in memory before the restart. |
| O11 | Delete a copy after its first restore | Not built. A split gives one snapshot to several children. A delete after the first child makes a failure that only the fake has. | Rejected. Copies stay until `clearFakeSnapshots()`. |

We did not try these options. Each has a reason:

| Option | Reason |
| --- | --- |
| Copy the SQLite file of the DO on the disk of the host | The DO must be stopped, the paths belong to Miniflare, and vitest isolates the storage of each test file. |
| Implement the snapshot API in a patched workerd | We would have to build and keep our own workerd. It is a possible contribution to workerd. |
| Keep the page-by-page import locally, and use snapshots on Cloudflare | This is the second code path that goal 1 forbids. |

## 5. What to adopt

1. **The test subclass (O3).** A test DO class calls `installFakeSnapshots(ctx, env)` before `super(ctx, env)`. The
   production class stays unchanged. `SnapshotPartitionDO` in `test/snapshot/snapshot-partition-do.ts` is the first
   such class. The DST wrapper `withDst` can call the same function.
2. **The `memory` backend (O6) as the default** of `pnpm test` and of DST runs.
3. **The `host` backend (O8)** when a person must read the copies, for example after a DST failure. Set
   `FOKOS_SNAPSHOT_BACKEND=host` and `FOKOS_KEEP_SNAPSHOTS=1`.
4. **The `do` backend (O7)** for a run without the vitest Node process, for example under `wrangler dev` (not tested).
5. **Later: a storage seam in the runtime.** The backup RFC proposes the type `FokosBackupStorage`, which the runtime
   takes as an option. Production passes `ctx.storage`. With that seam, a unit test can run the flow without a DO. The
   fake of this document can be the implementation for the DO tests. Both can exist.

## 6. How the tests use it

### 6.1 The files

| File | Contents |
| --- | --- |
| `test/snapshot/fake-snapshots.ts` | `installFakeSnapshots`, the copy and the restore, the text form, the three backends, `setFakeSnapshotBackend` and `clearFakeSnapshots`. |
| `test/snapshot/snapshot-partition-do.ts` | `SnapshotPartitionDO`: a `FokosDBPartitionDO` with the fake, and the test controls `debugSnapshot`, `debugRestoreFrom` and `debugRestart`. |
| `test/snapshot/snapshot-store-do.ts` | `SnapshotStoreDO`: the `do` backend. |
| `test/snapshot/snapshot-host.ts` | The `host` backend. It runs in the Node process of vitest. |
| `test/snapshot/cleanup-setup.ts` | A setup file that calls `clearFakeSnapshots()` after each test file. |
| `test/snapshot/fake-snapshots.test.ts` | The text form, each backend, the restore into a DO with data, cleanup, and the `kvData` limits. |
| `test/snapshot/snapshot-sizes.test.ts` | Partitions of 1 MiB, 100 MiB and 1 GiB in each backend, and the `kvData` overwrite of the identity. |

`wrangler.jsonc` adds the flag `durable_object_snapshots` and the bindings `SNAPSHOT_PARTITION_DO` and
`SNAPSHOT_STORE_DO`. `vitest.config.ts` adds the service binding `FOKOS_SNAPSHOT_HOST`.

### 6.2 The sequence of one restore

```text
source DO                         target DO (SnapshotPartitionDO)
─────────                         ───────────────────────────────
ctx.storage.snapshot()
  copy of every user table and
  every KV key -> backend
  handle { fakeSnapshotId, backend }
        ── handle over RPC ──────►  ctx.storage.onNextSessionRestore(handle, { kvData })
                                      check the kvData limits
                                      read the copy from the backend into memory
                                      return the undo bookmark
                                    ctx.abort()        (the caller restarts the DO)
                                    next start: installFakeSnapshots, before super()
                                      one transactionSync: drop all, write the copy, write kvData
                                      delete the alarm
                                    super(ctx, env)    (the production constructor)
```

### 6.3 The settings

| Variable | Effect |
| --- | --- |
| `FOKOS_SNAPSHOT_BACKEND=memory\|do\|host` | The default backend of a run. `setFakeSnapshotBackend()` changes it in a test. |
| `FOKOS_SNAPSHOT_DIR=<path>` | The directory of the `host` backend. The default is `<os.tmpdir()>/fokosdb-snapshots`. |
| `FOKOS_KEEP_SNAPSHOTS=1` | Cleanup renames each copy of the `host` backend to `<id>.kept/` in place of a delete. The code cannot read a kept copy. |
| `FOKOS_SNAPSHOT_SIZES=all` | `snapshot-sizes.test.ts` runs each size in each backend. Without it, it runs 1 MiB in each backend and 100 MiB in `memory`. |

### 6.4 The `kvData` overwrite

A copy holds the identity of the source (`__fokos/identity`), its repartition rows and its import state. A child must
not keep them. `snapshot-sizes.test.ts` shows the first part of a fix:

- `kvData` gives the copy the identity of the target. The restore writes `kvData` after the copied keys, so it
  replaces the copied identity.
- With the new identity, the copy serves the items of the source under its own name. It refuses the context of the
  source with `partition_context_mismatch`.
- Without the overwrite, the copy refuses the context of its own name.

The spec of split by snapshot must also say how the child removes the copied `fokos_` rows and the rows outside its
slice. This is the repartition form of R9 of the backup RFC.

### 6.5 Use in DST

- The restart step of the simulator (`__dstRestart()`, which calls `ctx.abort()`) also lands a scheduled restore. Thus
  the simulator decides when a restore lands.
- The fake makes these faults simple: `snapshot()` throws, a restore is scheduled and the DO never restarts, a second
  `onNextSessionRestore` replaces the first, and a restart occurs between the schedule and the next step of the flow.
- The calls of the `do` and `host` backends go outside the gates of the simulator. They model the platform, not
  FokosDB, so the simulator must not schedule them.
- The DST rule "the tests do not read SQLite tables" stays true. The fake reads SQLite, but it is part of the platform
  model, not a checker.

## 7. Where the fake differs from production

| Difference | Effect | What to do |
| --- | --- | --- |
| The copy starts one microtask after the call. Production includes the writes of the current atomic batch. | Close. The documentation does not give the exact position. | DST can select the position, so that the code does not depend on it. |
| No `snapshot({ bookmark })`, and no restore from a bookmark. | Backup needs both. Repartition does not. | Add them together with a fake `getCurrentBookmark()` that keeps a copy for each bookmark. |
| The handle is a plain object, so code can store it. | Code that stores a handle passes locally and fails on Cloudflare. | Refuse a handle that was read from storage, or use an `RpcTarget`. |
| The `kvData` byte count uses JSON. workerd uses V8 serialization. | A value near 3 KB can pass locally and fail on Cloudflare. | Keep a margin. |
| A restore lands at the next start. Production takes about 20 s at p90 and up to 65 s at p99. | Timing defects stay hidden. | DST holds the restart. A normal test can wait before the restart. |
| A copy lives until `clearFakeSnapshots()`. | A handle that the code uses too late still works locally. | Accept. The fake cannot see when the last holder of a handle ends. |
| A scheduled restore holds its copy in memory until the restart. | A 1 GiB restore needs about 1 GiB of memory in the isolate. | Accept. The restore before `super()` must be synchronous. |
| The fake does not copy alarms, and it deletes the alarm of the target. | This is the production behavior that the backup RFC states. | Confirm on Cloudflare. |

## 8. Measurements

The orb has 2 CPUs and 3.9 GB of memory. The DOs hold 64 KiB random blobs.

One snapshot of a 1 GiB DO, in seconds:

| Backend | `snapshot()` | `onNextSessionRestore()` | Restart and apply |
| --- | --- | --- | --- |
| `memory` | 1.0 | 0.0 | 4.5 |
| `do` | 17.9 | 5.8 | 5.2 |
| `host` | 6.8 | 3.7 | 5.0 |

`snapshot-sizes.test.ts` with `FOKOS_SNAPSHOT_SIZES=all` passes its 10 tests in 95 s, with a peak resident set of
1.4 GB. Each test includes the fill and a SHA-256 of each blob of the source and the target:

| Size | `memory` | `do` | `host` |
| --- | --- | --- | --- |
| 1 MiB | 0.13 s | 0.15 s | 0.13 s |
| 100 MiB | 1.6 s | 4.3 s | 3.1 s |
| 1 GiB | 15.6 s | 42.2 s | 24.3 s |

## 9. Next steps

1. Write the spec of split and promotion by snapshot in `docs/agent-plans/`: the child starts from the snapshot of its
   parent, `kvData` gives the identity and a marker, and the first start of the child removes what it does not own.
2. Put the snapshot calls behind `FokosBackupStorage` in the runtime.
3. Close the differences of section 7 that the spec needs.
4. Run a deployed check on Cloudflare with the flag. It must confirm the position of a snapshot, the alarm delete, and
   `kvData` before the constructor.

## 10. References

- [workerd `actor-state.h`](https://github.com/cloudflare/workerd/blob/v1.20261123.1/src/workerd/api/actor-state.h)
- [workerd `snapshot.h`](https://github.com/cloudflare/workerd/blob/v1.20261123.1/src/workerd/api/snapshot.h)
- [workerd `actor-cache.h`](https://github.com/cloudflare/workerd/blob/v1.20261123.1/src/workerd/io/actor-cache.h)
- [Durable Objects SQLite storage API](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/)
- [Durable Objects limits](https://developers.cloudflare.com/durable-objects/platform/limits/)
