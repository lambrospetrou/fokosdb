# RFC — Memory Allocation Optimizations across FokosDB

**State:** Draft
**Date:** 2026-09-20
**Author:** Lambros Petrou

**Status:** Nothing in this document is built.

## Table of contents

- [1. Overview and context](#1-overview-and-context)
- [2. Goals and requirements](#2-goals-and-requirements)
  - [2.1 In scope](#21-in-scope)
  - [2.2 Out of scope](#22-out-of-scope)
- [3. Milestones](#3-milestones)
- [4. Proposed solution](#4-proposed-solution)
  - [4.1 High-level overview](#41-high-level-overview)
  - [4.2 Technical details](#42-technical-details)
- [5. Alternative options](#5-alternative-options)
- [6. Frequently asked questions](#6-frequently-asked-questions)
- [7. References](#7-references)

## 1. Overview and context

FokosDB runs inside Cloudflare Workers and Durable Objects. The runtime uses the V8 JavaScript engine.
Garbage collection pauses in V8 increase tail latency under high write and read throughput.

The codebase currently allocates temporary objects and arrays on hot execution paths.
These paths include point reads, range queries, two-phase commit writes, and partition migration.
Each allocated object increases heap churn.

This document identifies eight areas where code can remove temporary allocations.
Applying these changes reduces memory pressure without changing public API behavior or durability guarantees.

## 2. Goals and requirements

### 2.1 In scope

- Remove temporary array allocations from single-row SQLite queries across the storage layer.
- Reuse a singleton byte buffer for empty key representations in `KeyCodec`.
- Avoid round-trip serialization and deserialization on the happy path of write transactions.
- Remove intermediate per-row wrapper objects during query page row collection.
- Remove intermediate tuple arrays in multi-item read transactions.
- Avoid base64 string allocations during migration byte accounting.
- Use zero-copy buffer views where key bytes are decoded.
- Pre-compute and cache expression statement bindings on compiled plans.

### 2.2 Out of scope

- Custom manual memory management or off-heap memory outside V8 TypedArrays.
- Changes to the wire protocol format of Workers RPC.
- Changes to SQLite storage table schemas.

## 3. Milestones

1. **Milestone 1 — Utility and codec allocations**: Add `one` and `tryOne` cursor helpers, singleton empty key, and
   zero-copy decoding in `KeyCodec`.
2. **Milestone 2 — Client transaction map allocations**: Replace intermediate tuple arrays in `transactGetItems` with
   direct iteration.
3. **Milestone 3 — Expression binding cache**: Cache materialized bindings on compiled update and query plans.
4. **Milestone 4 — Two-phase commit fast-path transfer**: Pass in-memory items and contexts into `drivePrepare` to avoid
   reloading from SQLite on the happy path.
5. **Milestone 5 — Migration byte accounting**: Replace per-item base64 string allocation with integer key hashes.
6. **Milestone 6 — Query scan row streaming**: Replace generator with candidate consumer callback in
   `scanQueryPage`.

## 4. Proposed solution

### 4.1 High-level overview

The proposed optimizations eliminate short-lived JavaScript allocations in hot loops and RPC handlers:

```
Hot Path                     Current Behavior                      Optimized Behavior
──────────────────────────────────────────────────────────────────────────────────────────────────
Single SQLite read           res.toArray()[0] (allocates Array)    one(res) / tryOne(res) (zero array allocation)
Empty sort key               new Uint8Array(0)                     EMPTY_KEY_BYTES singleton
Binary key decode            k.slice(1)                            k.subarray(1) zero-copy view
transactGetItems phases      new Map(array.map(r => [k, r]))       for..of set (zero tuple allocations)
Expression update execute    materializeBindings(plan.bindings)    plan.materializedBindings cached
2PC initiateWrite            Insert -> Reload SQL -> JSON.parse    Insert -> Pass memory refs to prepare
Migration applyItems         hk.toBase64() per item                KeyCodec.mapKey(hk) integer identity
Query scan iteration         yield { sk, estRowBytes, item, ... }  Consumer callback with deferred payload
```

### 4.2 Technical details

#### 4.2.1 Single-row SQLite queries (`one` and `tryOne`)

63 calls in `PartitionStore` and `TransactionCoordinatorDO` read single rows with this pattern:

```ts
const row = res.toArray()[0];
```

The `toArray()` method converts all cursor rows into a new JavaScript array.
For single-row queries, the runtime allocates an array of size 1 and discards it immediately.

Two utility functions must replace these calls:

```ts
export function tryOne<T>(cursor: Iterable<T>): T | undefined {
	for (const row of cursor) {
		return row;
	}
	return undefined;
}

export function one<T>(cursor: Iterable<T>, message?: string): T {
	for (const row of cursor) {
		return row;
	}
	invariant(false, message ?? "expected at least one row from query");
}
```

Both functions read the first item from the SQLite cursor and stop iteration.
They allocate zero intermediate arrays.

Use `tryOne` when the query result can be absent, such as `getItem` or `pendingLockFor`.
Use `one` when an invariant requires a row to exist, such as `COUNT(*)` or a confirmed transaction row.

#### 4.2.2 Static empty key bytes in `KeyCodec`

In `packages/fokosdb/src/shared/partition-topology/key-codec.ts`, `encodeOptional` allocates a new `Uint8Array` when
the sort key is absent:

```ts
function encodeOptional(key: string | Uint8Array | undefined): KeyBytes {
	return key === undefined ? asKeyBytes(new Uint8Array(0)) : encode(key);
}
```

Every point read and range lookup without a sort key triggers this allocation.
The module must declare one frozen singleton:

```ts
const EMPTY_KEY_BYTES = Object.freeze(asKeyBytes(new Uint8Array(0)));
```

When `key === undefined`, `encodeOptional` must return `EMPTY_KEY_BYTES`.

#### 4.2.3 Zero-copy buffer views in `KeyCodec.decode`

In `packages/fokosdb/src/shared/partition-topology/key-codec.ts`:

```ts
if (k.length > 0 && k[0] === BINARY_TAG) {
	return k.slice(1);
}
```

The `slice(1)` call copies the binary key into a new `Uint8Array`.
`decode` must return `k.subarray(1)` when callers read the buffer without mutating it.

#### 4.2.4 Direct Map insertion in `transactGetItems`

In `packages/fokosdb/src/client/db.ts`, `transactGetItems` matches phase 1 and phase 2 items:

```ts
const phase1ByKey = new Map(phase1Flat.map((r) => [itemIdentity(r), r]));
const phase2ByKey = new Map(phase2Flat.map((r) => [itemIdentity(r), r]));
```

For 100 items, `map()` allocates one outer array and 100 two-element tuple arrays per phase.
The code must populate the map directly:

```ts
const phase1ByKey = new Map<bigint, ReadForTransactionItemResultEncoded>();
for (const r of phase1Flat) {
	phase1ByKey.set(itemIdentity(r), r);
}
```

This change removes all intermediate array allocations.

#### 4.2.5 Pre-materialized expression bindings

In `packages/fokosdb/src/shared/partition/partition-store.ts`, `StatementTail` materializes bindings:

```ts
class StatementTail {
	constructor(plan: CompiledUpdatePlan) {
		this.#planBindings = materializeExpressionBindings(plan.bindings);
	}
}
```

Every update execution calls `materializeExpressionBindings` and iterates over the descriptor list.
The compiler must compute `materializedBindings` once during `compileUpdatePlan` and store the result on the plan.
`StatementTail` must read the pre-computed array directly from `plan.materializedBindings`.

#### 4.2.6 Fast-path two-phase commit memory transfer

In `packages/fokosdb/src/server/do-transaction-coordinator.ts`, `initiateWrite` writes state, items, and participants
to SQLite:

1. `initiateWrite` writes `tc_state`, `tc_items`, and `tc_participants` into SQLite.
2. It calls `drivePrepare(transactionId, idempotencyToken, coordinatorDoId)`.
3. `drivePrepare` calls `this.loadItems(transactionId)` and `this.loadParticipants(transactionId)`.
4. The SQLite engine reads the rows back from disk.
5. The method parses JSON strings for all condition plans, update plans, and partition contexts.

Write-ahead logging to SQLite before outbound network calls is required for durability.
However, reloading and parsing data from SQLite on the happy path is unnecessary.

`drivePrepare` must accept optional in-memory structures:

```ts
private async drivePrepare(
	transactionId: string,
	idempotencyToken: string,
	coordinatorDoId: string,
	requestBudgetMs?: number,
	inMemory?: {
		items: TcItemRow[];
		participants: TcParticipantRow[];
	},
): Promise<InitiateWriteResponseEncoded>
```

When `initiateWrite` calls `drivePrepare`, it passes the items and participants already held in memory.
When the coordinator restarts or an alarm fires, `inMemory` is absent. `drivePrepare` then loads from SQLite.

#### 4.2.7 Migration byte accounting with integer keys

In `packages/fokosdb/src/shared/partition/fokos-migration-host.ts`, `#applyItems` computes byte estimates per key:

```ts
const id = item.hk.toBase64({ alphabet: "base64url" });
const entry = bytesByKey.get(id);
```

For every item in a migration page, `toBase64` allocates a string.
The method must group by `KeyCodec.mapKey(item.hk)`, which returns a 64-bit integer, and store items in a
`Map<bigint, { hk: KeyBytes; bytes: number }>`.
If an integer hash collision occurs, the loop compares raw key bytes before merging the byte totals.

#### 4.2.8 Query scan row streaming with consumer callbacks

In `packages/fokosdb/src/shared/partition/partition-store.ts`, `scanQueryPage` wraps each SQLite row in a generator
object:

```ts
yield { sk, estRowBytes: row.est_row_bytes as number, matched, item, projected };
```

A scan that reads 1,000 items allocates 1,000 wrapper objects.
`scanQueryPage` also constructs the `StoredItem` object before `collectQueryPage` checks page budgets.
When a row exceeds the item budget or the byte budget, the collector rejects the candidate.
The allocated item object is discarded immediately.

`scanQueryPage` must use a callback consumer instead of a generator:

```ts
export type QueryCandidateConsumer = (
	sk: KeyBytes,
	estRowBytes: number,
	matched: boolean,
	decodePayload: () => StoredItem | ProjectedWireRow,
) => boolean;
```

`PartitionStore.scanQueryPage` executes the SQLite cursor loop and calls the consumer:

```ts
scanQueryPage(
	opts: RangeScanBounds & { limit: number; select: QuerySelect; plan: CompiledQueryPlan | null },
	consumer: QueryCandidateConsumer,
): SqlMetrics {
	const { sql, params } = queryScanStatement(opts);
	const cursor = this.#storage.sql.exec<Record<string, SqlStorageValue>>(sql, ...params);
	const entryCount = opts.plan?.projection?.names.length ?? 0;
	const mode = opts.select !== "projection" ? "none" : opts.plan?.projection ? "projected" : "item";

	for (const row of cursor) {
		const sk = fromSqlKey(row.sk as ArrayBuffer);
		const estRowBytes = row.est_row_bytes as number;
		const matched = opts.plan === null ? true : row.matched === 1;

		const shouldContinue = consumer(sk, estRowBytes, matched, () => {
			if (mode === "projected") {
				return decodeProjectedRow(row, entryCount);
			}
			return {
				hk: fromSqlKey(row.hk as ArrayBuffer),
				sk,
				data: fromSqlData(row.data as string | ArrayBuffer),
				kind: kindFromCode(row.data_kind as number),
				ttl_epoch_utc_seconds: row.ttl_epoch_utc_seconds as number | null,
				v: row.v as number,
				last_read_ts: row.last_read_ts as number,
				last_write_ts: row.last_write_ts as number,
			};
		});

		if (!shouldContinue) {
			break;
		}
	}

	return { rowsRead: cursor.rowsRead, rowsWritten: cursor.rowsWritten };
}
```

`collectQueryPage` checks evaluated budgets before it decodes payload data:

1. When remaining evaluated items or evaluated bytes are exceeded, the consumer returns `false`.
   The SQLite cursor loop breaks immediately.
2. When the row matches and the selection mode is `"projection"`, the consumer calls `decodePayload()`.
   It checks the response byte budget.
3. When the response byte budget is exceeded, the consumer returns `false`.
4. When all budgets pass, the consumer appends the materialized item and returns `true`.

This design has three benefits:
1. It allocates zero wrapper objects during iteration.
2. It removes the generator state machine in V8.
3. It defers payload decoding until after budget checks pass.

## 5. Alternative options

### 5.1 Manual buffer pooling for all keys

We considered a shared global `ArrayBuffer` pool for all key encoding operations.
This option was rejected. Reusable mutable buffers introduce race conditions when concurrent asynchronous tasks
read keys across `await` points.
The selected approach uses immutable views (`subarray`) and static constants (`EMPTY_KEY_BYTES`), which are safe
under concurrent asynchronous execution.

## 6. Frequently asked questions

### Will avoiding SQLite reload in 2PC reduce crash safety?

No. `initiateWrite` executes synchronous SQLite insertions inside `this.ctx.storage.transactionSync` before calling
`drivePrepare`.
The state is committed to SQLite disk before any network RPC starts.
Passing in-memory references avoids only the redundant read and JSON parse operations on the happy path.

### How do `one` and `tryOne` handle empty results?

When a query yields zero rows, `tryOne` returns `undefined`.
`one` throws a `FokosInternalError` with code `invariant_failed`.

## 7. References

- `docs/agent-plans/2026-08-30-bounded-stateful-transaction-coordination.md`
- `docs/agent-plans/2026-09-17-unified-repartition-flow.md`
- [TigerStyle Guide](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/TIGER_STYLE.md)
