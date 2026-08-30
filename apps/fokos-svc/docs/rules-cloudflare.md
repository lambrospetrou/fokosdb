# Cloudflare platform rules

Date: 2026-08-30
Status: **active; these rules describe the code as it is today**

The constraints this codebase designs against, and the invariants it holds when it uses the Cloudflare platform. This
document is not a summary of the platform documentation. It records what we decided, what we measured, and what we
worry about. Break a rule only with a written reason in the code, next to the place that breaks it.

The database rules — partition routing, the migration guard, the 2PC invariants — are in `AGENTS.md`. The testing rules
are in `docs/rules-testing.md`.

References:

- [Workers limits](https://developers.cloudflare.com/workers/platform/limits/)
- [Durable Objects limits](https://developers.cloudflare.com/durable-objects/platform/limits/)
- [Rules of Durable Objects](https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/)

## Table of contents

- [Ground rules](#ground-rules)
  - [Retrieve the limits; do not remember them](#retrieve-the-limits-do-not-remember-them)
  - [An error crossing a boundary keeps only its message](#an-error-crossing-a-boundary-keeps-only-its-message)
  - [Classify an error before you retry it](#classify-an-error-before-you-retry-it)
  - [Log with one structured object](#log-with-one-structured-object)
- [Architecture and capacity](#architecture-and-capacity)
  - [Count the network hops on the hot path](#count-the-network-hops-on-the-hot-path)
  - [A forward must teach the router](#a-forward-must-teach-the-router)
  - [Size a partition for migration, not for the platform limit](#size-a-partition-for-migration-not-for-the-platform-limit)
  - [Many objects means many cold starts](#many-objects-means-many-cold-starts)
  - [The throughput ceiling is chosen at table creation](#the-throughput-ceiling-is-chosen-at-table-creation)
  - [Bound every fan-out](#bound-every-fan-out)
  - [Every named object is a single point of failure](#every-named-object-is-a-single-point-of-failure)
  - [Any state that grows per request needs a bound](#any-state-that-grows-per-request-needs-a-bound)
- [Durable Objects](#durable-objects)
  - [A Durable Object knows nothing about itself](#a-durable-object-knows-nothing-about-itself)
  - [Name objects deterministically](#name-objects-deterministically)
  - [Only a DO class or FokosDB acquires a stub](#only-a-do-class-or-fokosdb-acquires-a-stub)
  - [In-memory state is a cache; storage is the truth](#in-memory-state-is-a-cache-storage-is-the-truth)
  - [Re-read state after every remote await](#re-read-state-after-every-remote-await)
  - [Write the decision before you send the call](#write-the-decision-before-you-send-the-call)
  - [An alarm is one timer per object](#an-alarm-is-one-timer-per-object)
  - [A setTimeout does not survive eviction](#a-settimeout-does-not-survive-eviction)
  - [Every background job is idempotent, crash-safe, and self-rescheduling](#every-background-job-is-idempotent-crash-safe-and-self-rescheduling)
  - [Keep RPC types flat, small, and serialisable](#keep-rpc-types-flat-small-and-serialisable)
- [Workers and the request path](#workers-and-the-request-path)
  - [Module scope is shared by every request in the isolate](#module-scope-is-shared-by-every-request-in-the-isolate)
  - [Encode and validate once, at the boundary](#encode-and-validate-once-at-the-boundary)
- [SQLite storage in a Durable Object](#sqlite-storage-in-a-durable-object)
  - [One class owns the SQL](#one-class-owns-the-sql)
  - [Migrations are additive, numbered, and run at startup](#migrations-are-additive-numbered-and-run-at-startup)
  - [Table design rules measured on this platform](#table-design-rules-measured-on-this-platform)
  - [Index for the queries that run on every commit](#index-for-the-queries-that-run-on-every-commit)
  - [Page with a row-value cursor](#page-with-a-row-value-cursor)
  - [Ask the cheapest question](#ask-the-cheapest-question)
  - [Convert types at the store boundary](#convert-types-at-the-store-boundary)
  - [SQL limits](#sql-limits)
- [Deployment](#deployment)

---

## Ground rules

### Retrieve the limits; do not remember them

Your knowledge of the platform is older than the platform. Read the product's `/platform/limits/` page before you write
code that depends on a limit. Every number in this document is a snapshot to verify, not a fact.

### An error crossing a boundary keeps only its message

Durable Object RPC carries the message. It does not carry the class, the `instanceof` identity, or a custom property.
When a caller must recognise an error, put a sentinel substring in the message, and export a predicate that tests for
it:

- `OVER_SIZE_SENTINEL` and `isPartitionExceededDatabaseSizeError` (`src/lib/do-partition.ts`).
- `FAST_PATH_FALLBACK_SENTINEL` and `isSinglePartitionFastPathFallbackError` (same file).
- `DESTROY_ABORT_SENTINEL` and `isDestroyAbortError` (`src/lib/cf-utils.ts`).

The sentinel is a contract. One factory builds the message, and the message keeps the sentinel.

A transport failure carries no sentinel. That is the point: a lost connection must never be read as a semantic answer.

### Classify an error before you retry it

Use `isErrorRetryable` for transport and platform errors, and `tryWhile` for the loop. Bound every loop.

Do not retry what a retry cannot fix. An over-size partition needs a split, and a split does not land inside a retry
budget of a few seconds. Do not retry a write that carries no idempotency token.

### Log with one structured object

One object per call, with a `message` field and the DO's `logParams()`. Log both `String(error)` and the error object:
the first is readable, the second keeps the fields a log viewer can index.

---

## Architecture and capacity

### Count the network hops on the hot path

Every hop is a round trip between colos. The client Worker and the object it calls are usually not in the same place,
and no amount of local optimisation pays that back.

- A point read or write is one hop: `FokosDB` to the owning partition.
- A transaction through the coordinator is three hops or more: client to coordinator, coordinator to each participant
  for prepare, then again for commit.
- The single-partition fast path exists to remove those hops when one partition owns every item
  (`#readSnapshotFastPath` and the single-shot write path in `src/lib/db.ts`).

A new operation that adds a hop to the read path needs a fast path, or a written reason why it does not.

### A forward must teach the router

A request that lands on a parent after a split is forwarded down to the owner. That forward is an extra hop, and paying
it twice for the same key is a defect.

Every response carries the serving leaf's ancestors in `meta._internal`, and each forwarding node feeds them to
`recordForwardResult` so later requests skip levels. `forwardCount` in the response meta is the number to watch: it
must trend to zero for a stable topology.

### Size a partition for migration, not for the platform limit

The platform allows 10 GB per object. We split a hash partition at 100 MB and a range partition at 500 MB
(`PartitionContextCreator.create` defaults).

The limit that matters is not storage, it is the time to move the data. A split migrates the whole partition to its
children over RPC in about 20 MB batches, while writes to the migrating child are rejected. A multi-gigabyte partition
turns that window from seconds into a long outage for those keys. Keep a partition well under 1 GB.

The same reasoning applies to any new object we add: choose its size ceiling from how long it takes to rebuild, split,
or copy it, not from what the platform tolerates.

### Many objects means many cold starts

A first request to an object pays construction: the schema migrations run, and the in-memory state is loaded from
storage under `blockConcurrencyWhile`.

Consequences we design for:

- Keep the constructor cheap and local. No remote call belongs in it. Work that is not needed for correctness runs
  after it, without blocking (the colo fetch in `src/lib/do-partition.ts`).
- A user request must wake only the objects that own its data. An operation that touches every partition to answer one
  key is a design error, not a performance detail.
- `rootTreesN` is the number of objects that exist from the start. A high value buys throughput and costs a cold start
  per object on a cold table.
- A fan-out to N objects has the cold start of the slowest of them, not the average.

### The throughput ceiling is chosen at table creation

One object sustains roughly 1,000 requests per second. `rootTreesN` fixes the partition count and cannot change after
initialization, so the table's write ceiling is set when it is created. Splits add depth, not root fan-out.

Say the intended ceiling in the spec of any new object pool, and say what happens when it is reached.

### Bound every fan-out

A Worker invocation gets 6 simultaneous outgoing connections. A wider fan-out serialises, and a fan-out from inside an
object also serialises against that object's own request flow.

Page every scan, budget every batch, and give every fan-out a maximum width. `PageBudget` and `collectBatch` are the
two tools.

### Every named object is a single point of failure

An object is one process. While it is unavailable, everything it owns is unavailable, and any lock it holds stays held.

- Size the blast radius on purpose. Say in the spec what stops when this object stops.
- Never build a global coordinator.
- Never make one object the only holder of a recovery path. A transaction that can be resolved by one coordinator alone
  is stuck for as long as that coordinator is.

### Any state that grows per request needs a bound

State with no garbage collector and no written bound is a defect, not a follow-up. Say which job deletes it, and what
the ceiling is at the peak rate the design accepts.

---

## Durable Objects

### A Durable Object knows nothing about itself

Workers RPC cannot configure an object at construction, so an object learns what it is only from what the caller sends.

**Every RPC carries the `PartitionContext`**, and the object validates it against the one it stored
(`ensurePartitionContext`). An RPC that omits the context is a defect, not a shortcut.

The same rule drives `internalInitFromSplit`: initialisation is explicit and idempotent, and a second call with
conflicting options throws instead of overwriting.

### Name objects deterministically

Use `idFromName(name)`, or the `static getByName` helper on the class. A deterministic name makes routing reproducible
and populates `ctx.id.name` inside the object, so logs can name themselves.

Use a unique id only when something else stores the mapping — the read-transaction coordinator does, because it holds
no state and nothing has to find it again.

An idempotency token is a name. Changing how a name is derived, or the size of a shard pool that a name hashes into,
breaks every replay in flight.

### Only a DO class or FokosDB acquires a stub

Helper classes never resolve a stub. They receive a narrow interface (`PartitionPeer`) or a factory from the object
that owns them. The rule keeps the set of places that can start a remote call small enough to audit.

### In-memory state is a cache; storage is the truth

An object is evicted on inactivity and on an uncaught exception, and every field goes with it. Write the authoritative
value to SQLite or `storage.kv`, and rebuild the cache in the constructor.

Fields prefixed `__testing__` exist for tests. No product logic may read one.

### Re-read state after every remote await

Storage gates do not cover non-storage I/O. Every `await` on a remote call is a point where another event interleaves,
so state read before it may be stale after it.

Never hold a remote call inside `blockConcurrencyWhile`. It stops the object.

### Write the decision before you send the call

Every state transition is written before the outbound RPC that acts on it. An object that stops between the two steps
then wakes with a decision it can finish, instead of an effect it cannot explain.

Use `ctx.storage.transactionSync` for a multi-statement change. The callback is synchronous — no `await` inside it.

### An alarm is one timer per object

Setting an alarm replaces the alarm that is set, so every caller goes through the earliest-wins guard:

```ts
private async ensureAlarmSet(targetMs: number): Promise<void> {
	const existing = await this.ctx.storage.getAlarm();
	if (existing === null || targetMs < existing) {
		await this.ctx.storage.setAlarm(targetMs);
	}
}
```

A direct `setAlarm` can push an earlier alarm later and stall recovery.

The handler must be idempotent, must read its work from storage, and must page long work with a cursor rather than run
past the alarm wall-time limit. Delete the alarm before wiping storage.

### A setTimeout does not survive eviction

The fast path schedules background work with `setTimeout`; the alarm is the fallback that runs the same work when the
object is evicted first. Both call the same `runBackgroundWork()`.

Guard the timer against a thundering herd: keep the scheduled target time, skip a schedule that is not earlier, and
reset the marker after a bounded delay, so a slow run cannot block every future run.

### Every background job is idempotent, crash-safe, and self-rescheduling

1. A job is safe to run again while a previous run is still in flight.
2. A job that crashes must not stop the other jobs, and must resume without losing data.
3. A job that fails logs the failure and reschedules, so progress happens eventually.

Each job sits in its own `try/catch`, so a failed migration does not skip the split job or the stale-transaction job.

### Keep RPC types flat, small, and serialisable

- No recursive type in an RPC signature. A recursive `JsonValue` makes the RPC type machinery instantiate without end,
  so JSON travels as text and is parsed at the public boundary.
- A branded type loses its brand over the wire. Re-brand on arrival with the zero-cost cast, never with a copy
  (`KeyCodec.asKeyBytes`).
- Internal routing fields stop at the public boundary. `db.ts` deletes `_internal` at runtime, because structural
  typing accepts an extra property.
- Budget the payload: migration batches target about 20 MB against a 32 MB RPC ceiling and 128 MB of isolate memory.

---

## Workers and the request path

### Module scope is shared by every request in the isolate

Top-level state lives as long as the isolate, and every object and request in that isolate reads it. Use it only for a
value that is identical for every caller and safe to reuse, such as the colo cache in `src/lib/cf-utils.ts`. Never for
per-request or per-object state.

### Encode and validate once, at the boundary

The Worker validates the request; `FokosDB` encodes it. Nothing below re-validates or re-encodes.

- `encodeItemData` stringifies JSON exactly once, and `KeyCodec` encodes every key, so an object receives only
  `string | Uint8Array` plus a kind discriminant.
- `db.ts` is also the single decode boundary: it parses JSON text once and drops internal fields.
- Use `strictObject` in a variant schema, so a field belonging to another variant becomes a named 400 instead of a
  silently stripped field.

---

## SQLite storage in a Durable Object

### One class owns the SQL

`PartitionStore` owns every statement against the partition tables, plus the schema migrations and the row-size
estimators. No other class touches those tables. The same rule holds for the coordinator's tables.

Methods are single-purpose and named for intent. Raw SQL is acceptable because it lives in one file.

The store does not decide transaction boundaries. The caller composes atomicity with `transactionSync`.

### Migrations are additive, numbered, and run at startup

Use `SQLSchemaMigrations` from `durable-utils/sql-migrations`, with a monotonically increasing `idMonotonicInc` and a
description. Run them inside the constructor's `blockConcurrencyWhile`.

A shipped migration is immutable. Change the schema with a new entry.

### Table design rules measured on this platform

- **Use `STRICT`.** It rejects a value of the wrong type at write time.
- **Do not use `WITHOUT ROWID` for a table with wide rows.** It stores rows in an index B-tree whose inline payload
  limit is about 1002 bytes on a 4 KiB page, so every larger row takes a private overflow page it cannot share. Measured
  on Durable Object storage: 1000-byte data cost 4683 physical bytes per row. A rowid table still enforces uniqueness
  through the primary-key autoindex.
- **Declare `NOT NULL` explicitly.** A rowid table does not imply it from the primary key.
- **Put the wide column last.** SQLite reads pages for a row until it has the columns the query needs, so a trailing
  `data` column keeps the hot metadata reads off the overflow pages.
- **Use `ANY` for a polymorphic value column**, with an integer discriminant. It keeps the physical storage class: TEXT
  for text, BLOB for bytes, and BLOB for JSONB.
- **Do not use a generated column in an index you need to cover.** SQLite refuses to treat such an index as covering.
  Compute the value in both writers instead, from one shared SQL expression.

### Index for the queries that run on every commit

Add an index when a query filters on a column that is not a leading primary-key column. `pending_transactions` carries
an index on `(transaction_id, hk, sk)` because `transaction_id` is the third primary-key column and cannot be seeked
alone. Without it, committing one transaction costs a scan of every pending row in the partition. Measured: 147 page
reads drop to 3.

Spell the extra key columns out. A rowid table appends only the rowid to an index entry, so an index that relied on the
primary key coming along for free stops covering when the table stops being `WITHOUT ROWID`.

Record the measurement in a comment next to the index. The next reader must be able to see why the index exists.

### Page with a row-value cursor

A paged scan must compare the whole key tuple:

```sql
SELECT ... FROM items WHERE (hk, sk) > (?, ?) ORDER BY hk, sk LIMIT ?
```

The equivalent nested form `hk > ? OR (hk = ? AND sk > ?)` cannot seek. SQLite then re-checks every remaining row, and
each page rescans from the start of the index. Measured: 337 page reads for a late page against 4 for the row-value
form.

Other paging rules, from `src/lib/partition/batch-scan.ts`:

- The cursor advances past every scanned row, matched or not, so a resumed scan never re-evaluates a filtered row.
- The first matched row is always included, even when it alone exceeds the byte budget. One oversized row must not stall
  progress.
- A non-null `nextCursor` means the budget stopped the scan. A null one means the scan is complete.
- Checkpoint the cursor in storage, so a crash resumes instead of restarting.

### Ask the cheapest question

Use `SELECT 1 ... LIMIT 1` for existence. `COUNT(*)` walks the whole index.

Use `LIMIT` inside a subquery when you only need to know whether a count reaches a threshold.

Return `{ rowsRead, rowsWritten }` from a statement whose cost is reported in the RPC `meta`, and read
`sql.databaseSize` for the storage figure that split policy uses.

### Convert types at the store boundary

- SQLite returns a BLOB column as an `ArrayBuffer`. Every row-reading method converts it to a `Uint8Array` view before
  the value leaves the store.
- BLOB comparison is a memcmp. `KeyCodec.compare` produces that same total order, so key order in JS and key order in
  SQL agree. Keep it that way.
- The SQL layer cannot bind a JS `BigInt` ("Cannot convert a BigInt value to a number"), and `Number()` drops precision
  above 2^53. Store such a value as hex text (`src/lib/transaction-idempotency.ts`).
- Decode JSONB to JSON text in SQL (`json(data)`) for a public read, so JS never handles raw JSONB. A migration read
  copies the JSONB blob verbatim.
- Interpolate into SQL only a constant the code owns, such as a fixed enum code. Every value from a caller binds as a
  parameter.

### SQL limits

Current figures to verify: 2 MB for a string, BLOB, or row; 100 KB for a statement; 100 bound parameters per query; 32
function arguments; 100 columns per table.

The serialised bloom filter is sized against the 2 MB row limit, with a target near 1 MB and headroom to 1.5 MB
(`src/lib/do-partition.ts`).

---

## Deployment

Two Wrangler projects live in this service:

- `wrangler.jsonc` — the library worker. Never deployed. It gives vitest and `wrangler types` an entry point.
- `src/examples/http-api/wrangler.jsonc` — the deployable example API worker, with its own assets, secrets, generated
  types, and local state under `.wrangler/`.

Rules:

- Change a binding in one file, then run `npm run cf-typegen`, which regenerates both type files.
- Deploy with the secrets file: `npm run deploy`. Secrets never enter the repository.
- `observability.logs` is on. Invocation logs and traces are off because they are too noisy at the current volume. Turn
  a trace on for an investigation, then turn it off.
