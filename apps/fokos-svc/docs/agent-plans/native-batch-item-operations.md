# Native Batch Item Operations — Implementation Plan

Status: **implemented on `feat/batch-item-ops`**. This plan defines native, non-transactional
`BatchGet` / `BatchWrite` operations alongside `putItem`/`getItem`/`deleteItem`/`queryItems`, modeled on
the DynamoDB `BatchGetItem` / `BatchWriteItem` operations and adapted to fokosdb's hash-partition +
promoted-range topology. It is the completion record for that work: the public contract, the invariants,
the milestone history, and the review-driven deviations.

The scope is intentionally narrower than DynamoDB parity and broader than a client-side helper loop. The
core operations are native `PartitionDO` RPCs that preserve the existing partition routing, migration,
split, lock, and promotion behavior. `FokosStd` (M5) adds optional client-side chunking and bounded retry
on top of the native operations.

## Source hierarchy and quality bar

Implementation decisions are resolved in this order:

1. **Existing FokosDB behavior and style.** Match the public API shape in `FokosDB`, the HTTP RPC shape in
   `src/index.ts`, the validation style in `transaction-limits.ts`, the split/migration rules in
   `PartitionDO`, and the level of detail in the existing `queryItems` / `KeyCodec` plans.
2. **Official DynamoDB BatchGetItem / BatchWriteItem docs**, as the semantic baseline for limits,
   duplicate-key rejection, non-atomic writes, and `UnprocessedKeys` / `UnprocessedItems`:
   - <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html>
   - <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html>
3. **Current Cloudflare Workers / Durable Objects limits**, used only to choose safe server-side bounds:
   - <https://developers.cloudflare.com/workers/platform/limits/>
   - <https://developers.cloudflare.com/durable-objects/platform/limits/>

This is not a thin wrapper over single-item calls. It is a native, well-factored implementation with
tests around operation semantics, limits, split/migration guards, and partial-failure behavior.

## 1. DynamoDB parity matrix

| DynamoDB feature | fokos decision |
|---|---|
| `BatchGetItem` / `BatchWriteItem` request shape | **Adopt** as `batchGetItems({ items })` / `batchWriteItems({ operations })`; keys are `string \| Uint8Array`, encoded via `KeyCodec` at the boundary like every other op |
| Write op set (`PutRequest` / `DeleteRequest`) | **Adopt** `put` / `delete` only. No `check` (that is a transaction op) |
| Non-atomic, no rollback | **Adopt**. Per-item independence across *and* within a partition |
| `ConditionExpression` on batch writes | **Dropped** — DynamoDB has none either; conditional writes stay on `transactWriteItems` |
| `UnprocessedItems` / `UnprocessedKeys` | **Adopt** as retryable per-item entries, correlated by `inputIndex` |
| 25-write / 100-read item caps | **Adopt** as `MAX_BATCH_WRITE_ITEMS` / `MAX_BATCH_GET_ITEMS`; the *core* call enforces them, the `FokosStd` helper chunks above them |
| 16 MB request cap | **Replaced** by fokos byte ceilings: a per-batch `MAX_BATCH_PAYLOAD_BYTES` and a per-op `MAX_BATCH_FORWARDED_SUB_BATCH_BYTES` chosen under the workerd RPC limit |
| Duplicate keys in one request | **Adopt DynamoDB**: reject the whole batch at preflight |
| Strongly-consistent reads | **Per-item strong** (each key is owned by one DO). A multi-key batch is **not** a global snapshot — that is `transactGetItems` |
| Returned attributes on writes | **Dropped** — `BatchWriteItem` returns none; processed entries carry keys only, no version |
| Auto-chunking + retry helper (DocumentClient) | **`FokosStd`** (M5), a separate client-side layer, not the core wire API |

A batch request is therefore: **a flat list of per-item keys (get) or put/delete operations (write),
preflight-validated, routed per item, applied non-atomically, with retryable per-item failures correlated
by `inputIndex`.**

## 2. Public API

Keys use the same `string | Uint8Array` shape as the single-item and transaction APIs, encoded via
`KeyCodec` at the `FokosDB` boundary.

```ts
type BatchGetItemsOptions = {
	items: Array<{ hashKey: string | Uint8Array; sortKey?: string | Uint8Array }>;
};

type BatchWriteItemsOptions = {
	operations: Array<
		| { operation: "put"; hashKey: string | Uint8Array; sortKey?: string | Uint8Array;
		    data: string | Uint8Array; ttlSeconds?: number; ttlEpochUTCSeconds?: number }
		| { operation: "delete"; hashKey: string | Uint8Array; sortKey?: string | Uint8Array }
	>;
};
```

Result types (`src/lib/types.ts`):

```ts
type BatchRetryableFailureReason =
	| { type: "pending_lock"; conflictingTransactionId?: string }
	| { type: "partition_over_limit" }
	| { type: "transient_error"; message?: string };

type BatchItemsMeta = {
	requestedCount: number; processedCount: number; unprocessedCount: number;
	rowsRead: number; rowsWritten: number; forwardCount: number; partitionsVisited: number;
};

type BatchGetProcessedItem =
	| {
			inputIndex: number;
			found: true;
			item: {
				hashKey: string | Uint8Array;
				sortKey?: string | Uint8Array;
				data: string | Uint8Array;
				ttlEpochUTCSeconds?: number;
				version: number;
			};
	  }
	| { inputIndex: number; found: false; item: ItemKey };

type BatchGetItemsResult = {
	items: BatchGetProcessedItem[];
	unprocessedKeys: Array<{ inputIndex: number; item: ItemKey; reason: BatchRetryableFailureReason }>;
	meta: BatchItemsMeta;
	partitionMetas: Array<OperationMetrics & PartitionInfo>;
};

type BatchWriteItemsResult = {
	processedItems: Array<{ inputIndex: number; operation: "put" | "delete"; item: ItemKey }>;
	unprocessedItems: Array<{
		inputIndex: number;
		operation: "put" | "delete";
		item: ItemKey;
		reason: BatchRetryableFailureReason;
	}>;
	meta: BatchItemsMeta;
	partitionMetas: Array<OperationMetrics & PartitionInfo>;
};
```

`inputIndex` is the correlation authority. Results may echo keys for ergonomics, but a caller must be able
to reconstruct the full outcome from `inputIndex` alone. `batchWriteItems` deliberately returns **no
per-applied version** — it is a partial-success/retry API, not a read-after-write; callers that need a
version read the item afterward.

## 3. Correctness invariants

1. **`inputIndex` is the correlation authority.** Every processed and unprocessed entry carries the
   caller's original input position. The core assigns it per call; `FokosStd` re-maps chunk-local indices
   back to global ones. A returned index outside the request is **result corruption** — fail loud, never
   silently fall back (`db.ts` echo helpers throw on an unknown `inputIndex`).
2. **Hard input errors throw before any work.** Empty batch, duplicate `(hashKey, sortKey)`, unsupported
   op, missing `put` data, oversized item count / total payload / single forwarded op — all reject at
   preflight, never as retryable `Unprocessed*`.
3. **`batchWriteItems` is non-atomic and condition-free**, across *and* within a partition. Atomicity is
   `transactWriteItems`'s job.
4. **Writes respect the two concurrent state machines, never bypass them.** `ensureMigration` is called
   (throwing variant) before routing and re-checked at each forwarded child; a batch **write** to a
   pending-locked item or a migrating-child write RPC failure returns a retryable `Unprocessed*` entry, not
   a wrong-partition write. (Reads instead tolerate migration via parent-read — invariant 5; `pending_lock`
   is write-only.) The local apply re-checks per-item ownership so a mid-batch split/promotion cannot strand
   a write on a former owner.
5. **`batchGetItems` is per-item strongly consistent, not a snapshot.** Reads tolerate migration by
   reading the parent (mirrors `getItem`), never throwing.
6. **`partitionMetas` records partitions that performed local read/write work; pure forwarding-only routers
   are excluded.** `forwardCount` is subtree-cumulative; `requestedCount` is the global N; processed/
   unprocessed counts are the *final* state; `FokosStd` concatenates a total visit trail across
   chunks/retries (no per-attempt count double-counting).
7. **Retry covers only unprocessed entries, and is not version-idempotent.** Re-applying a put/delete is
   safe (last-write-wins) but bumps the version again.

## 4. File Map

| Area | File |
|---|---|
| Public types (options, results, `BatchItemsMeta`, failure union) | `src/lib/types.ts` |
| Internal RPC types (`*RpcRequest` / `*RpcResult`, `KeyBytes` keys) | `src/lib/batch-types.ts` |
| Validation + shared estimators + batch constants | `src/lib/transaction-limits.ts` |
| Client orchestration (group-by-root, fan-out, echo, aggregate) | `src/lib/db.ts` |
| `PartitionDO` RPCs + local bodies + split/range forwarding | `src/lib/do-partition.ts` |
| HTTP RPC actions, valibot schemas, serializers, `runBatchRpc` | `src/index.ts` |
| Client-side chunking + bounded retry helper | `src/lib/fokos-std.ts` |
| Tests | `src/lib/{db,do-partition,batch-types,transaction-limits,fokos-std}.test.ts`, `test/http-batch.test.ts` |

No SQL migrations. The `items` table and existing store methods are reused as-is.

## 5. Milestones (as implemented, M0–M5)

Each milestone compiled and tested independently and was gate-reviewed before the next began.

### M0 — Validation, result types, constants
- **Scope:** public + RPC types, batch constants, `validateBatchGetItems` / `validateBatchWriteOperations`
  (shared low-level helpers, batch-specific wording — not `validateTransactWriteOperations`). `FokosDB`
  methods validate + encode, then throw "not implemented yet".
- **Files:** `types.ts`, `batch-types.ts`, `transaction-limits.ts`, `db.ts`.
- **Acceptance:** empty/over-max/duplicate/oversized reject at preflight; duplicate identity computed on
  **encoded `KeyBytes`** (absent sort key = `[]`), matching the store; conditions / `check` rejected.
- **Verification:** `transaction-limits.test.ts`, `batch-types.test.ts`, `db.test.ts`.

### M1 — BatchGet end-to-end
- **Scope:** `FokosDB.batchGetItems` groups by initial `pickPartition` root, one RPC per root group via
  `Promise.allSettled`, preserves the request `inputIndex` through root/child fan-out, echoes original
  keys, aggregates `meta`/`partitionMetas`. `PartitionDO.batchGetItems` adds migration parent-read,
  `groupItemsByRouting` fan-out to children, leaf `partitionMetas`, cumulative `forwardCount`.
- **Files:** `db.ts`, `do-partition.ts` (`batchGetItems`, `batchGetItemsDirect`, `batchGetItemsLocal`).
- **Acceptance:** input order preserved; `found: false` for misses; child failures → `UnprocessedKeys`;
  no `getItem` regression (shared `partitionMeta` helper carries every field).
- **Verification:** `db.test.ts`, `do-partition.test.ts`.

### M2 — BatchWrite, local / single-partition path
- **Scope:** `PartitionDO.batchWriteItems` calls `ensureMigration` (throwing) **before** routing; the local
  loop reuses the single-item bodies — `pendingLockFor` → `pending_lock`, `upsertItem` +
  `maybeQueuePromotion` + `checkSplits` for put, store delete watermark for delete — per item,
  non-atomic, siblings preserved.
- **Files:** `db.ts`, `do-partition.ts` (`batchWriteItems`, `batchWriteItemsLocal`).
- **Acceptance:** pending-lock → retryable with conflicting tx id; no versions in processed entries;
  mixed put/delete; partial failure does not hide successes.
- **Verification:** `do-partition.test.ts`, `db.test.ts`.

### M3 — BatchWrite split/range forwarding + migration partial-failure
- **Scope:** forward child sub-batches via `Promise.allSettled`; chunk forwarded ops by the forwarded byte
  guard; per-item **ownership re-check before every local apply** so a mid-batch split/promotion re-routes
  rather than mis-writes (no TOCTOU — re-check→write is synchronous, the only `await` is `checkSplits`
  after the write). Migrating child → retryable `Unprocessed*`.
- **Files:** `do-partition.ts`, `transaction-limits.ts` (split estimators).
- **Acceptance:** a single-key item across a completed split forwards and is visible; migrating-child and
  over-limit failures are retryable, not bypassed.
- **Verification:** `do-partition.test.ts` (split/migration scaffolding).

### M4 — HTTP API surface
- **Scope:** `batchGetItems` / `batchWriteItems` RPC actions in `src/index.ts`: strict valibot schemas
  (`put`/`delete` variant, no conditions / `clientRequestToken`, count bounds), full-result serializers
  (`encodeData`, binary-key guard), and `runBatchRpc` mapping `fokos:`-prefixed validation throws to HTTP
  400 while leaving internal `fokos/` errors as 500.
- **Files:** `src/index.ts`.
- **Acceptance:** client validation errors return 400 (not 500); `check`/conditions/`clientRequestToken`
  rejected; data round-trips via `encodeData`; binary keys fail loud.
- **Verification:** `test/http-batch.test.ts`.

### M5 — `FokosStd` chunking + bounded retry
- **Scope:** `FokosStd` composes a `FokosDBAPI` and adds `batchGetAll` / `batchWriteAll`: full-input
  duplicate rejection **before** chunking; chunk by min(item-count, payload-bytes); bounded
  exponential-backoff retry (injectable `sleep`/`random`/`isRetryableError`) of unprocessed entries *and*
  retryable thrown chunk calls; chunk-local→global `inputIndex` re-map on every attempt; honest aggregate
  meta (cumulative work, final counts, total-visit `partitionMetas`).
- **Files:** `src/lib/fokos-std.ts`. Shared identity/estimator helpers are imported from
  `transaction-limits.ts`, not duplicated.
- **Acceptance:** unbounded input chunks correctly; retries are bounded and return the remaining
  unprocessed; no cross-chunk duplicate slips through; not exported from the Worker entry (consistent with
  `FokosDB`).
- **Verification:** `fokos-std.test.ts`.

## 6. Decisions locked

- **Request shape:** flat per-item list (`items` / `operations`); the chunking convenience form lives in
  `FokosStd`, not the core wire API.
- **Op set:** `put` / `delete` only; no conditions; no `clientRequestToken`; not routed through the TC.
- **Correlation:** `inputIndex`, primary and required; key-echo is for ergonomics only.
- **No per-applied versions** from the core write result.
- **Duplicate keys:** rejected at preflight, on encoded `KeyBytes` identity (absent sort key = `[]`).
- **Atomicity:** none. `transactWriteItems` owns atomic writes; `transactGetItems` owns snapshot reads.
- **Failure model:** hard input errors throw; runtime per-item failures return retryable `Unprocessed*`.
- **Guards:** `ensureMigration` before routing + on forwarded children; per-item ownership re-check on the
  local apply.
- **Limits:** in `transaction-limits.ts` (below), enforced server-side; DynamoDB 25/100 caps + retry live
  in `FokosStd`.
- **Bounding:** real subrequest growth is bounded by item-count caps plus DO-side split/sub-batch guards —
  **not** by counting `FokosDB`'s initial `pickPartition` roots (`rootTreesN` is user-configurable and
  split/range expansion happens behind the root). See Appendix B (MF1).

## 7. Limits

Server-side constants (`src/lib/transaction-limits.ts`), conservative and tunable, chosen under the
workerd RPC limit rather than relying on the platform as the first line of defense:

- `MAX_BATCH_GET_ITEMS = 100`, `MAX_BATCH_WRITE_ITEMS = 25` — per-call item caps.
- `MAX_BATCH_PAYLOAD_BYTES = 4 MB` — per-batch total payload ceiling.
- `MAX_BATCH_FORWARDED_SUB_BATCH_BYTES = 1 MB` — per-op forwarded ceiling; a single op over this rejects at
  preflight (deterministic oversize must not masquerade as retryable). The **same estimator** sizes both
  the preflight check (on raw keys) and the DO-side forwarded chunking (on encoded `KeyBytes`) so the two
  cannot drift — see Appendix B.

Re-check current Cloudflare Workers / Durable Objects limits before tuning byte and DO-side constants.

## 8. Failure taxonomy

Per-item retryable failures are a discriminated union (`src/lib/types.ts`):

- `pending_lock` — carries the conflicting transaction id, matching the single-item write diagnostic;
- `partition_over_limit` — the topology `reject` / retry-later decision, or an `unplaceable` item;
- `transient_error` — a transient child RPC failure (carries the message).

Hard input errors throw before work starts; they never appear as unprocessed entries.

## 9. Test checklist

- `FokosDB.batchGetItems`: preserves input order; `found: false` for misses; transient child failure →
  `UnprocessedKeys`; rejects oversized item count / payload.
- `FokosDB.batchWriteItems`: rejects empty / duplicate (absent sort key = empty key) / unsupported op /
  conditions / oversized count / oversized single forwarded op; preserves correlation across routed groups;
  mixed put/delete; pending-lock and over-limit → retryable; siblings stay visible.
- `PartitionDO`: migration guard called before routing and in forwarded children; single-key item across a
  completed split forwards and is visible.
- HTTP: valid shapes accepted, invalid rejected; client validation → 400, internal → 500.
- `FokosStd`: chunking by count + bytes; cross-chunk duplicate rejection; bounded retry returns remaining
  unprocessed; chunk-local→global `inputIndex` re-map across retries.
- Existing transaction, query, migration, promotion, and split tests still pass.

## Appendix A — Original GitHub issue draft (historical task anchor)

> Kept as the issue text this work was scoped from. The body above is the authoritative contract.

**Title:** Native non-transactional BatchGet/BatchWrite operations.

**Body:** FokosDB's README lists "Batch item operations (non-transactions)" as a TODO. Add native
`batchGetItems({ items })` and `batchWriteItems({ operations })` — a DynamoDB-shaped, FokosDB-native
contract, not full API parity — plus HTTP RPC actions and tests for public/HTTP behavior, validation,
partial failures, split/migration routing, unprocessed retry, and request limits.

**Non-goals:** full DynamoDB parity; conditional batch writes; atomic batch writes; GSI/index behavior;
an automatic retry helper outside `FokosStd`.

## Appendix B — Deviations / review fixes

Changes made during dual independent review, after the initial milestone landed:

- **MF1 — fan-out cap removed (M0).** An initial client-side `MAX_BATCH_PARTITION_FANOUT` counted distinct
  `pickPartition` roots, which is bounded by `rootTreesN` (so a no-op at default and a spurious rejection
  when `rootTreesN > 10`) and does not reflect server-side split/range expansion. Removed; item-count caps
  plus M2/M3 DO-side guards are the real subrequest backstop.
- **MF2 — meta enriched (M0).** `BatchItemsMeta` gained `rowsRead`/`rowsWritten`/`forwardCount`/
  `partitionsVisited` and the results carry `partitionMetas`, matching `queryItems`/`putItem` rather than a
  counts-only shape.
- **Forwarded byte guard + empty-sortKey regression (M3).** A single op over the forwarded ceiling now
  hard-rejects at preflight. The first fix reused a *validating* estimator on already-encoded keys, which
  rejected the `[]` absent-sort-key sentinel and double-encoded; split into
  `estimateBatchWriteForwardedOperationBytes` (raw) and `estimateEncodedBatchWriteForwardedOperationBytes`
  (encoded), the former defined in terms of the latter so preflight and DO measure the identical bytes.
- **`migration_in_progress` removed.** The reason was declared but never emitted (migrating-child failures
  surface as `transient_error`); the dead union member was dropped. Emitting it via a typed migration error
  is a possible follow-up.
- **BatchGet fail-loud `inputIndex`.** The echo helpers now throw on an unknown `inputIndex` instead of
  silently falling back, matching the write path — a missing index means result corruption, not a valid
  partial failure.
- **Cleanup.** Removed the `BatchRetryableFailure` identity alias; added a non-version-idempotence comment
  on the `FokosStd` retry path.

**Known follow-ups (separate tickets, not this work):** extend the `fokos:`→400 mapping to the
`transactWriteItems` HTTP route; decide `ttlSeconds` honor-or-reject consistently across `putItem` and
batch (currently forwarded then dropped at the DO, matching pre-existing `putItem` behavior); possible
extraction of the batch local write body into a participant module.
