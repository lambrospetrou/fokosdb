# API consistency and correctness audit

Date: 2026-08-08
Scope: `src/lib/db.ts`, `src/lib/types.ts`, `src/lib/transaction-types.ts`, `src/lib/do-partition.ts`,
`src/lib/do-transaction-coordinator.ts`, `src/lib/partition/*`, `src/lib/query/*`,
`src/lib/partition-topology/*`, `src/index.ts`.

This report finds bugs and inconsistencies in the code that exists today. It does not add new features.
`tsc --noEmit` is clean at the time of this audit.

---

## 1. Summary

The core engine (routing, splits, promotion, migration, key codec, query paging) is coherent.
Its rules are written down and the code obeys them.

The problems are at the two seams:

1. **The transaction API and the non-transaction API are two different products.** They do not agree on
   the error model, the result envelope, the metadata, the ordering, the limits, or the TTL support.
   This is Section 3.
2. **The timestamp-ordering layer has real correctness holes.** The largest one is that a
   non-transactional write can move an item's `last_transaction_ts` backwards. This is Section 4.

The five items to fix first:

| # | Item | Where | Status |
|---|------|-------|--------|
| B1 | Non-tx write moves `last_transaction_ts` backwards. Lost update is possible. | `partition/partition-store.ts:372` | **DONE** |
| B2 | A decided transaction returns an exception to the client instead of its outcome. | `do-transaction-coordinator.ts:228-249` | **DONE** |
| B3 | Partition backpressure (`"reject"`) becomes an invariant crash on `txPrepare`. | `do-partition.ts:1566-1568` | **DONE** |
| B6 | The coordinator never deletes transaction rows. Storage and alarm cost grow forever. | `do-transaction-coordinator.ts` | open |
| A1 | Conditions throw on `putItem` but return a `cancelled` value on `transactWriteItems`. | `db.ts` / `transaction-types.ts:150` | open |

---

## 2. Method

I read every non-test source file. I traced each public method from `db.ts` down to the SQL, and back
up. I compared each pair of related paths (put vs transactional put, get vs transactional get, split
forwarding vs transaction routing) statement by statement. Line numbers are from the current `main`
(`ac1d907`).

---

## 3. API inconsistencies

### A1. Two different error models — the largest inconsistency

Non-transactional operations signal a business failure with a **thrown exception**:

- A failed condition throws from `evaluateConditionsOnItem` (`partition/partition-store.ts:94-114`).
- A pending transaction lock throws `"item is locked by an in-progress transaction"`
  (`do-partition.ts:388`, `do-partition.ts:451`).
- Size backpressure throws `"partition exceeded its limits"` (`do-partition.ts:1515`).
- A migration in progress throws `"Partition split in progress"` (`do-partition.ts:1390`).

Transactional operations signal the same failures with a **returned value**:

```ts
{ outcome: "cancelled", transactionId, idempotencyToken, reason: RejectionReason }
```

So the caller must write two different failure handlers for the same business event. The transactional
one is the better design: it is typed, it is machine-readable, and it names the key that failed. The
non-transactional one loses the key and the failure class in a string.

**Recommendation.** Pick one model. I recommend the structured one, and use a typed error class for
the non-transactional path so both carry the same `RejectionReason` union:

```ts
class FokosConditionFailed extends FokosError { reason: RejectionReason }
```

This also closes the README item "Proper structured errors thrown to differentiate user vs server errors".

### A2. Four different result envelopes

| Method | Envelope |
|---|---|
| `putItem` | `{ item, version, meta }` |
| `deleteItem` | `{ item, deleted, meta }` |
| `getItem` | `{ found, item: {...data, kind, version}, meta }` |
| `queryItems` | `{ items: [{hashKey, sortKey, data, kind, version}], count, cursor, meta, partitionMetas }` |
| `transactWriteItems` | `{ outcome, transactionId, idempotencyToken }` (`items` dropped — see A5) |
| `transactGetItems` | `{ outcome, items: [{found, hashKey, sortKey, data, kind, lastCommittedTs, hasPendingWrite}] }` |

`getItem` nests the item under `item`. `queryItems` and `transactGetItems` flatten it. The nested and
the flat form carry the same fields. A caller cannot write one item decoder.

### A3. Transactions return no `meta`

Every non-transactional result carries `OperationMetrics & PartitionInfo`: `rowsRead`, `rowsWritten`,
`databaseSize`, `servedByActorId`, `servedByActorName`, `servedByPartitionId`, `forwardCount`,
`hashDepth`, `rangeDepth` (`types.ts:118-165`).

`transactWriteItems` and `transactGetItems` return nothing of the kind. The HTTP layer shows this: it
sets `dbItemMeta` for `putItem`, `getItem`, `deleteItem` and `queryItems`, but not for the two
transaction routes (`index.ts:288-319`). You cannot observe, bill, or debug a transaction the way you
observe every other operation.

### A4. `transactGetItems` leaks 2PC internals and drops the version — ✅ FIXED (with B4)

Fixed together with B4, since they are one change: making the participant return `v` for the conflict
check is exactly what supplies the public `version`. `ReadForTransactionItemResultOf` gained a second
type parameter carrying the TC-only fields, so `…Encoded` (participant→TC) keeps `lastCommittedTs` /
`hasPendingWrite` while the public `ReadForTransactionItemResult` has neither; `db.ts` destructures
them away at the boundary. Found items now carry `version` and `ttlEpochUTCSeconds`, matching
`getItem`. The description below is kept as the record of the defect.

**This is a breaking response change** — `lastCommittedTs` and `hasPendingWrite` are gone from
`transactGetItems`. `tsc` caught the one consumer (an assertion in `test/transactions.test.ts`), which
now asserts `version` and that both internals are absent.

---

`ReadForTransactionItemResult` (`transaction-types.ts:85-108`) returns:

- `lastCommittedTs` — an internal timestamp-ordering value.
- `hasPendingWrite` — an internal lock state. It is always `false` in a `committed` outcome, because
  the coordinator aborts otherwise (`do-transaction-coordinator.ts:538-540`). So the field is dead
  weight in the only outcome where the caller sees it.

And it does **not** return:

- `version` (`v`) — but `v` is the only attribute that `attribute_equals` can test
  (`types.ts:35-38`). So you cannot do read-then-conditional-write with `transactGetItems`. You must
  use `getItem` for the read, which defeats the point of a consistent multi-item read.
- `ttlEpochUTCSeconds`, which `getItem` and `queryItems` both return.

### A5. `transactWriteItems` returns items in key order, not request order — ✅ FIXED

Fixed by deletion: the `committed` response no longer carries `items` at all. A write transaction is
all-or-nothing, so `committed` already says every operation applied, and the echo told the caller only
what it had just sent — in the wrong order, with `check` and `delete` presented as writes. DynamoDB's
`TransactWriteItems` answers the same way: consumed capacity and item collection metrics, nothing
item-shaped, and `ReturnValues` is not even accepted inside a transaction.

Three things fell out of the removal:

- `loadFinalResponse` no longer reads `tc_items` on the committed path — that read existed only to
  build the echo.
- `db.ts` has no decode step left in `transactWriteItems`; it returns the TC response directly.
- `InitiateWriteResponseEncoded` and `InitiateWriteResponse` were identical once the `KeyBytes` items
  were gone, so they collapsed into one type. The write path now has no encoded/public split.

**Still open, recorded as a FIXME on `InitiateWriteResponse`:** the `cancelled` branch reports ONE
`RejectionReason` for the whole transaction, so a caller with 100 operations cannot tell which one
failed unless the reason carries a key (`transient_error` and `clock_skew` carry none). DynamoDB
returns `CancellationReasons` — one entry per operation, in REQUEST ORDER, `Code: "None"` for the ones
that were fine — and raises it as a typed `TransactionCanceledException`. Both halves are wanted: the
positional reason array, and typed errors sharing the `RejectionReason` union with the
non-transactional path (A1). That is also what would let a size-rejected prepare say so instead of
reporting `transient_error` (B3's remaining scope).

So the positional guarantee DynamoDB actually makes for writes is on the failure path, and we do not
have it yet. The success path no longer needs one.

### A6. `transactGetItems` returns items in partition-group order — ✅ FIXED

Fixed in the TC, where the order was lost, so the guarantee holds for every `initiateRead` caller and
`db.ts` stays a 1:1 map. The final loop now walks `request.items` instead of `phase1Flat`, looking each
key up in a phase-1 AND a phase-2 map keyed by `KeyCodec.pairKey` — so restoring the order and checking
the conflict are the same pass, and the phase-1 map costs nothing extra (phase 2 already built one).
The guarantee is written on `InitiateReadResponseEncoded` / `InitiateReadResponse`: `items[i]` answers
`request.items[i]`, one entry per requested position, duplicates included.

Two consequences worth naming:

- A requested key with **no** reply is now caught. The old loop iterated the replies, so a dropped item
  produced a short array that the caller would silently mis-align. It aborts with `transient_error`.
- A key asked for twice is answered twice, at both positions. `validateTransactGetItemCount` does not
  reject duplicates, so this had to be defined rather than left to the fan-out.

The regression test (`test/transactions.test.ts`, "transactGetItems returns items positionally matched
to the request") asks for 4 keys from each of 3 partitions in interleaved order — an order that a
group-ordered response cannot produce — and was confirmed to fail against the old code. Ordinary key
sets do NOT catch this: with one key per partition, group order and request order coincide.

Only every other key is written, so the test also pins the case nothing covered before: an ABSENT item
occupies its own position and must not shorten the array and shift every later item onto the wrong key.
That half was verified by a second mutation — dropping the `found:false` push in
`readForTransactionLocal` — which the test catches as an aborted read.

A5, the write half, was settled the other way: `transactWriteItems` returns no item array at all, so
there is no order for it to get wrong.

---

`initiateRead` groups by `partitionContext.doName`, then flattens with `phase1Flat.push(...r.value.items)`
(`do-transaction-coordinator.ts:504-536`) and returns `phase1Flat` (`:578`). The order is the order of
the partition map, not the caller's order. DynamoDB guarantees positional correspondence. Nothing here
does. The caller must re-match on keys, and cannot even do that safely for binary keys (see B5).

### A7. Request shapes disagree — ✅ FIXED

Three renames and one type change, all breaking:

| Was | Is |
|---|---|
| `transactWriteItems({ operations })` | `transactWriteItems({ items })`, and `InitiateWriteRequest.items` on the wire |
| `queryItems({ queries: [{ sort }] })` | `queryItems({ queries: [{ sortKeyCondition }] })` |
| inline option shapes in `db.ts` | `TransactWriteItemsOptions`, `TransactGetItemsOptions`, `TransactWriteItem` in `transaction-types.ts`, re-exported from `types.ts` |

`queries` stays `queries`, against Section 6, which proposed `items` for all three. The rule chosen
instead: **the field names what the list contains.** A query is not an item — it returns many — so
`queries` is the accurate name and `items` would describe the result.

`TransactWriteItem` is now a union on `operation` instead of a flat struct with everything optional:

- `put` requires `data`,
- `delete` and `check` accept no `data`,
- `check` requires at least one condition.

`validateTransactWriteOperations` enforces the same three rules at runtime for the HTTP surface, and
`index.ts` uses `v.variant("operation", …)` over `strictObject` variants so a field from the wrong
variant is a 400 that names it. Two of the three rules were unenforced before: `data` on a `delete`
or a `check` was stored in `tc_items` and `pending_transactions` and then ignored (Section 5,
now closed), and a `check` with no conditions asserted nothing.

`FokosDBAPI` gained `ItemQuerier` and `ItemTransactor`, so the interface now covers all six methods
instead of put/get/delete only.

`KeyCodec.pairForLog(hk, sk)` was added for the error messages, replacing three hand-built
`hk=…, sk=…` renderings.

Not fixed, and not defects: there is no non-transactional `check` (DynamoDB has none either — a
conditional write with no write is `putItem` with `conditions`), and `batch` remains a README roadmap
feature. `scanIndexForward` keeps its DynamoDB name.

The description below is kept as the record.

---

- `transactWriteItems({ operations })` vs `transactGetItems({ items })` vs `queryItems({ queries })`.
  Three names for "the list of things to act on", and two of them are in the same feature.
- `queryItems` names the sort-key predicate `sort` (`types.ts:210`). Every other method names the key
  `sortKey`.
- The transaction operation type is an inline discriminant field (`operation: "put" | "delete" | "check"`),
  while the non-transactional API uses separate methods. There is no `check` in the non-transactional
  API, and no `batch` at all.

### A8. TTL is accepted everywhere and honoured nowhere

Three separate defects on one feature:

1. `db.ts:122-125` throws `"TTL expiration not yet implemented"` **only when both** `ttlSeconds` and
   `ttlEpochUTCSeconds` are given. The condition is `&&`. So the check is really a
   "both provided" check with a wrong message — and `index.ts:47-50` already does that check properly
   with a proper message.
2. `ttlSeconds` alone passes every layer and is **silently dropped**. It is never converted to an epoch
   and never stored. `apiPutItem` only reads `opts.ttlEpochUTCSeconds` (`do-partition.ts:408`).
3. A **transactional put always writes `ttlEpochUtcSeconds: null`** (`partition/transaction-participant.ts:193`).
   `TransactionItem` has no TTL field at all (`transaction-types.ts:21-31`). So a transactional put on
   an item that has an expiry silently clears the expiry, and you can never set an expiry inside a
   transaction.

Item 3 is a data-loss bug today, not only after TTL enforcement lands.

### A9. Limits apply only to transactional writes — ✅ FIXED

`transaction-limits.ts:10-11` sets `MAX_ITEMS_PER_TRANSACTION = 100` and `MAX_PAYLOAD_BYTES = 4 MB`.

- `putItem` has **no** payload limit at all. Only the key sizes are capped (`db.ts:37-38`).
- `transactGetItems` has **no** item-count limit (`db.ts:198-207`). A caller can send 100000 keys and
  fan out to every partition, twice (two phases).

**Fix.** A new `MAX_ITEM_BYTES = 400 KB` (DynamoDB parity) applies to EVERY write path — `putItem` and
each operation inside `transactWriteItems` — so the same item is accepted or rejected identically
through either API. `MAX_PAYLOAD_BYTES` stays what it was: the 4 MB **total** for a transaction.
Both are still needed; the per-item cap alone does not bound a transaction, since 100 items at the
item limit is far over 4 MB.

`putItem` measures the ENCODED data (`db.ts`), so a json payload is capped by the text actually
stored — the same number `transactWriteItems` measures.

`validateTransactGetItemCount` gives the read path the empty-set and 100-item checks the write path
had, and runs before any per-item key encoding so an oversized request fails fast.

**Note on the value.** 400 KB is a judgment call, chosen for DynamoDB parity and matching the
"start stricter, raise later" comment on the key-size ceilings. It is one constant. It DOES tighten
the transactional path: a single 4 MB put was previously legal because the only cap was the
transaction total. Two tests asserted exactly that and were rewritten to exercise the total cap with
ten 400 KB items instead.

### A10. `queryItems` skips key validation — ✅ FIXED

`putItem`, `getItem`, `deleteItem`, `transactWriteItems` and `transactGetItems` all call
`validateItemKeys`. `queryItems` does not (`db.ts:239-247`); it goes straight to `encodeHashKey`.
So a hash key that contains `\0` is rejected on write and accepted on query.

**Fix.** `queryItems` now calls `validateItemKeys(q.hashKey)` per sub-query.

Sort-key BOUNDS could not simply reuse `validateItemKeys`: it rejects an empty key, but
`begins_with: ""` is a legitimate query meaning "every sort key" (`sk-interval.ts:134-136`). So the
content rules (NUL, lone surrogate) were split out into `validateKeyContent`, which `validateItemKeys`
now calls, and the bounds get the content rules WITHOUT the emptiness rule.

The bounds are validated by passing a wrapping encoder, `encodeSortBound`, into
`normalizeSkInterval`. Every bound already flows through that encoder, so `between` and `range` — which
each carry two — are covered without enumerating the `SortKeyCondition` variants at the call site.

### A11. `_internal` is documented as internal and is returned over HTTP — ✅ FIXED

Fixed by splitting the type, not by remembering to delete a field: `PartitionInfo` (public,
`types.ts`) no longer declares `_internal` at all, and the new `PartitionInfoInternal`
(`partition-topology/types.ts`) is `PartitionInfo` plus that field. Every PartitionDO RPC response and
every `withSplitForwarding` / `forwardToRangeRootPartition` generic now names the internal type, so the
routing state still rides the DO→DO hops that need it — `recordForwardResult` seeds the
`range_hierarchy` cache from it — and the public results cannot name it.

`RangeAncestorInfo` moved to `partition-topology/types.ts` with it. It is routing state, and `types.ts`
holds public types only (the rule A13 established).

The type split alone does NOT stop the leak, and it is worth being explicit about why: TypeScript's
structural typing accepts an object carrying extra properties wherever a `PartitionInfo` is expected,
so the value would still have been serialized. `db.ts` therefore strips at runtime too, through one
`publicMeta()` helper applied at all five public exits — `putItem`, both `getItem` branches,
`deleteItem`, and each entry of `queryItems`' `partitionMetas`. The type split is what makes a NEW exit
that forgets the helper a visible mistake rather than an invisible one.

`index.ts` needed no change: it serializes whatever `db.ts` returns, so the HTTP leak closed with the
library one.

**Test.** `db.test.ts`, "strips _internal from every meta a public method returns", covers all five
exits and also asserts the rest of the meta survived. Mutation-confirmed: making `publicMeta` return
its argument unchanged fails it with `expected { rowsRead: 1, … } to not have property "_internal"`.
DO-level assertions in `do-partition.test.ts` still assert `_internal` IS present on RPC responses,
which is now the stated contract rather than an accident.

The description below is kept as the record.

---

`PartitionInfo._internal.rangeAncestors` was marked
`INTERNAL_ONLY: Not to be exposed to the final responses` (`types.ts:154-165`).

Nothing stripped it. `index.ts` passes `result` straight to `c.json(...)` for `putItem`, `deleteItem`,
`getItem` and `queryItems`. Because `RangeAncestorInfo.startBoundary`/`endBoundary` are `Uint8Array`,
they serialise as `{"0":97,"1":98}` objects. So the HTTP response contains internal routing state, in a
broken encoding, on every request served by a range partition.

### A12. Reads survive a migration; transactional reads do not

`apiGetItem` and `apiQueryItems` call `ensureMigration(op, false)` and fall back to reading from the
parent (`do-partition.ts:494-509`, `do-partition.ts:530-540`).

`txReadForTransaction` calls `ensureMigration("readForTransaction")` with the default
`throwIfMigrating = true` and throws (`do-partition.ts:1243`). A plain read works during a split; the
consistent read of the same key fails. `txCancel` throws too (`do-partition.ts:1203`), so a transaction
that must be cancelled during a migration cannot be, and the coordinator leaves it in `CANCELLING`
until the migration finishes.

The block on `txCommit`/`txCancel` is defensible — cancelling a lock that has not been migrated yet
would let the migration resurrect it. But the read path has no such hazard and should use the same
parent fallback as `apiGetItem`.

### A13. Four PartitionDO RPCs declare public key types and carry encoded keys — ✅ FIXED

Re-checked 2026-08-09, after A4/A5/A6/A7 and B5. The defect had survived untouched, and it was wider
than first written: it was on the **input** side as well as the output side.

**What landed.** The four item RPCs now declare `KeyBytes`, matching `QueryItemsRpcRequest` and every
transaction type. `sortKey` is always present; the empty `KeyBytes` is the absent sentinel.

| Was | Is |
|---|---|
| `apiPutItem(ctx, EncodedPutItemOptions) → PutItemResult` | `apiPutItem(ctx, PutItemRpcRequest) → PutItemRpcResponse` |
| `apiDeleteItem(ctx, DeleteItemOptions) → DeleteItemResult` | `apiDeleteItem(ctx, DeleteItemRpcRequest) → DeleteItemRpcResponse` |
| `apiGetItem` / `internalGetItemDirect` `(ctx, GetItemOptions) → GetItemResultEncoded` | `(ctx, GetItemRpcRequest) → GetItemRpcResponse` |

The new types live in `do-partition.ts` beside `QueryItemsRpcRequest`, so the DO's wire types sit with
the DO. `EncodedPutItemOptions` and `GetItemResultEncoded` are gone from `types.ts`, which now holds
public types only.

Five consequences:

- **`keyIn` and `optKeyIn` are deleted.** They existed only to resolve `string | Uint8Array`, and that
  is what made a `Uint8Array` mean "already encoded" while a `string` meant "encode me". Three further
  `keyIn` calls on values already typed `KeyBytes` went with them — a `KeyBytes` needs no re-brand.
- **No response carries a key.** `db.ts` answers with the caller's own, which are the only keys the
  caller can recognise. The public results are unchanged.
- **`GetItemResultOf<D>` is gone.** One generic served the DO type and the public type; once the DO
  type has no keys they had genuinely diverged, so `GetItemResult` and `GetItemRpcResponse` are now
  separate declarations.
- **`db.ts` names every field it sends** instead of spreading `...opts`. That makes `ttlSeconds`
  visibly not forwarded, with a comment saying so. It does not fix A8; it stops hiding it.
- `putItem` and `deleteItem` in `db.ts` gained their `PutItemResult` / `DeleteItemResult` return
  annotations, which they never had.

**Tests.** 196 call sites passed string keys directly and now pass `kb(...)`; `kb` was widened to
`(s?: string) => KeyCodec.encodeOptional(s)` in both test files, so `kb()` is the absent sort key.
Seven DO-level assertions on the echoed key were removed. The one coverage that removal would have
lost — an absent sortKey reading back as `undefined` — moved to `db.test.ts`, where it belongs, with a
second test pinning that a binary key comes back as the caller's raw bytes and not the `0xFF`-tagged
form. That second test is the A13 hazard stated as an assertion.

**The transaction coordinator was checked too, and needed nothing.** `initiateWrite`, `initiateRead`
and `recoverTransaction` are `KeyBytes` in and out; `tc_items` stores BLOBs, materialized with
`keyFromBlob` and compared with `KeyCodec.pairKey`. The TC never decodes a key.

The description below is kept as the record.

---

**Survey of every operation, both levels.**

`db.ts` is clean. Every public method either echoes the caller's own keys (`putItem`, `getItem`,
`deleteItem` — `db.ts:123`, `:138`, `:142`, `:153`) or decodes `KeyBytes` at the exit
(`queryItems` `db.ts:302-303`, `transactGetItems` `db.ts:222-224`). Nothing public leaks the wire form.

At the `PartitionDO` level, only four RPCs are dishonest:

| RPC | Declared keys | Actual keys |
|---|---|---|
| `apiPutItem` (`:378`) | `EncodedPutItemOptions` / `PutItemResult` — `string \| Uint8Array` | `KeyBytes` in, `KeyBytes` echoed out (`:422`) |
| `apiDeleteItem` (`:443`) | `DeleteItemOptions` / `DeleteItemResult` — same | `KeyBytes` in, `KeyBytes` echoed out (`:477`) |
| `apiGetItem` (`:498`) / `readItemLocally` (`:1599`) | `GetItemOptions` / `GetItemResultEncoded` — same | `KeyBytes` in, `KeyBytes` echoed out (`:1617`) |
| `internalGetItemDirect` (`:530`) | `GetItemOptions` | `KeyBytes` |

Everything else already declares what it carries: `QueryItemsRpcRequest.hashKey` is `KeyBytes`
(`:98`), `QueryItemsRpcResponse` returns `MigratedItem` with `hk`/`sk` as `KeyBytes`, and all four
transaction RPCs are `KeyBytes` in and out.

**Correction to the original text.** It said the transaction path "decodes properly inside the
participant". That stopped being true with B5: `ReadForTransactionItemResultEncoded` now carries
`KeyBytes`, and `db.ts` decodes at the single public exit. `RejectionReason` is the one deliberate
exception, because it is persisted and replayed. So the contrast is no longer "encoded here, decoded
there" — it is that every path now carries `KeyBytes` end to end, and these four are the only ones
whose types do not say so.

**The input half is the hazard, and it is new to this report.** `keyIn` (`:1289-1296`) resolves the
ambiguity by giving the two members of `string | Uint8Array` different meanings:

```ts
private static keyIn(k: string | Uint8Array): KeyBytes {
	return typeof k === "string" ? KeyCodec.encode(k) : KeyCodec.asKeyBytes(k);
}
```

A string is treated as an unencoded public key and is encoded. A `Uint8Array` is treated as
already-canonical and is passed through. So a caller that hands `PartitionDO` a genuine public BINARY
key gets it stored WITHOUT its `0xFF` tag — a different key from the one `putItem` would have written
for the same input, silently. `PartitionDO` is public and subclassable (`index.ts:333`), so this is
reachable. `do-partition.test.ts` already calls `internalGetItemDirect` both ways — `"hk"` at `:1404`
and `kb("alice")` at `:1986` — which is only safe because both are text keys.

**The output half is dead weight.** `db.ts` overwrites the echoed key with the caller's original on
every path, so the wire form is serialized across the RPC boundary and then discarded.

---

## 4. Correctness bugs

### B1. A non-transactional write can move `last_transaction_ts` backwards — HIGH — ✅ FIXED

Fixed: `upsertItem` now uses `MAX(last_transaction_ts, excluded.last_transaction_ts)`. Two regression
tests were added and both were confirmed to fail against the old SQL —
`partition-store.test.ts` ("upsertItem never lowers last_transaction_ts, but still applies the write")
and `transaction-participant.test.ts` ("still rejects a superseded transaction after a
non-transactional write with a lower timestamp"). `deleteItem`'s watermark path needed no change:
`bumpMaxDeletedTs` already used `MAX`. The description below is kept as the record of the defect.

---

`upsertItem` wrote:

```sql
ON CONFLICT(hk, sk) DO UPDATE SET ... last_transaction_ts = excluded.last_transaction_ts
```

(`partition/partition-store.ts:372`) — an unconditional overwrite.

Compare `bumpItemLastTransactionTs`, used by the transactional `check` operation, which correctly uses
`MAX(last_transaction_ts, ?)` (`partition/partition-store.ts:433`).

The failure sequence:

1. A coordinator's clock is ahead. It stamps a transaction `T = now + 4000ms`. The participant accepts
   it: `prepareLocal` only rejects timestamps more than `MAX_CLOCK_SKEW_MS = 5000` ahead
   (`partition/transaction-participant.ts:54`).
2. The transaction commits. The item now has `last_transaction_ts = now + 4000`.
3. A non-transactional `putItem` runs. It writes `lastTransactionTs: Date.now()` (`do-partition.ts:409`),
   which is **lower**. The watermark drops by ~4 seconds.
4. A second transaction, stamped anywhere in `(now, now + 4000]`, now passes the
   `transactionTimestamp <= itemRow.last_transaction_ts` check (`partition/transaction-participant.ts:103`)
   and commits over the newer non-transactional write.

Result: the non-transactional write is lost, and the serialisation order is violated.

**Fix.** Use `MAX(last_transaction_ts, excluded.last_transaction_ts)` in `upsertItem`, and the same in
`deleteItem`'s watermark path. This makes the item watermark monotonic, which is what the whole
timestamp-ordering scheme assumes.

`MAX` does not make the timestamp drift ahead of the wall clock: it never yields a value above
`max(what is already stored, Date.now())`, and what is already stored is bounded by
`Date.now() + MAX_CLOCK_SKEW_MS`. Drift needs an increment (`MAX(existing + 1, now)`), which this fix
does not use.

**What the ATC 2023 paper does (§3.3, §4).** DynamoDB keeps the same invariant by construction, and it
is worth reading before choosing a wider fix:

- Every write updates the item timestamp: *"DynamoDB records the timestamp of the write operation with
  every item. All write operations including singleton writes and writes within TransactWriteItems
  update the item timestamp."*
- Prepare accepts only if *"the transaction's timestamp is greater than the item's timestamp indicating
  when it was last written"* — the same check as `transaction-participant.ts:103`.
- A singleton write gets its timestamp **from the storage node** (our `PartitionDO`), not from a
  coordinator, and when a prepared transaction exists the node deliberately assigns *"a write timestamp
  that is earlier than the timestamps of any transactions in the prepared state"*, so the singleton
  write serialises **before** the pending transaction instead of aborting it. Our design note in
  `docs/ideas/dynamodb-distributed-transactions.md:200` says the opposite (pick a timestamp *higher*, to
  force the pending transaction to abort). The paper's direction is the better one — it keeps both
  operations alive. Either way, neither lowers an already-stored timestamp.
- The stale-transaction case is handled by the Thomas write rule, not by an overwrite: *"If a write
  operation that is part of a transaction arrives at a storage node that has already performed a write
  [...] with a later write timestamp, this transaction can still be accepted [...]. If this transaction
  is committed, its write operation is ignored."* We reject instead, which is safe and simpler; the
  paper notes this only matters for full overwrites, not partial modify operations.

### B2. A decided transaction can return an exception instead of its outcome — HIGH — ✅ FIXED

Fixed: `loadFinalResponse` now switches on the decision — `PREPARED`/`COMMITTING`/`COMMITTED` ⇒
`committed`, `CANCELLING`/`CANCELLED` ⇒ `cancelled` (falling back to `transient_error` if the reason
row is somehow absent), and only `CREATED`/`PREPARING` throw, with a `never` exhaustiveness guard.
`do-transaction-coordinator.test.ts` covers all seven states; 6 of its 8 cases were confirmed to fail
against the old code (the 2 that passed are `COMMITTED`/`CANCELLED`, which were already right).

Note the paper does **not** prescribe this. DynamoDB's `TransactWriteItem` (Listing 2) simply calls
`waitForAllCommitsToComplete()` and returns SUCCESS — it has no "give up" branch, because its storage
nodes are replicated with leader failover and a recovery manager resumes stalled transactions. We kept
the wait (it preserves read-your-writes on the happy path); what changed is only what we report when
the retry budget runs out. The rule applied is the 2PC invariant itself: once every participant has
voted to accept, the outcome can no longer change, so reporting anything else is false.

Caveat now documented in the code: returning `committed` while one participant is unreachable means a
read of *that* partition may not see the write until the alarm finishes the commit. That gap is
unavoidable — if the participant is unreachable, no answer makes the data readable — and `committed`
is the truthful one. The description below is kept as the record of the defect.

---

`loadFinalResponse` (`do-transaction-coordinator.ts:228-249`) only produced a response when
`tc_state.state` is `COMMITTED` or `CANCELLED`. Otherwise it throws
`"transaction ... still in progress, retry later"`.

But the state machine reaches `COMMITTED` only when **every participant** has confirmed
(`:365-375`), and reaches `CANCELLED` only when every participant has confirmed the cancel (`:409-425`).

Two bad paths follow:

- `drivePrepare` (`:298-313`): all participants accepted, so the transaction is past the point of no
  return and *will* commit. If one participant is slow and `runCommit` exhausts its 10 retries, the
  state stays `COMMITTING` and the client gets an exception for a transaction that is already
  irrevocably committed. The client cannot tell this from a real failure, and if it retries without a
  `clientRequestToken` it will run a second transaction.
- `drivePrepare` (`:315-321`): a participant rejected, so the decision is `cancelled` and the reason is
  already durable in `rejection_reason_json`. If cleanup of one participant is slow, the client again
  gets an exception instead of the correct, final `cancelled` answer.

**Fix.** Return the response from the **decision**, not from the cleanup:
`PREPARED`/`COMMITTING`/`COMMITTED` ⇒ `committed`; `CANCELLING`/`CANCELLED` ⇒ `cancelled`. Leave the
remaining participant cleanup to the alarm, which already exists (`:581-631`).

Related: the client currently waits for every participant to acknowledge the commit before it gets a
reply. With `nextAttempt <= 10` and `maxDelayMs: 2000`, a single unresponsive partition can hold the
caller for ~20 seconds. Answering on the decision fixes the latency too.

### B3. Backpressure becomes an invariant crash on every transaction path — HIGH — ✅ FIXED

`shouldAllow` returns `"reject"` for two legitimate reasons:

- the hash partition is over 110% of `maxSizeMb` (`partition-topology/split-policy.ts:189-194`),
- the range partition is over 110% of `maxSizeMb` (`:432-437`),

plus one genuine routing bug (a sort key outside the owned range, `:426-429`).

The non-transactional path handles this: `withSplitForwarding` turns it into
`"partition exceeded its limits, please retry later"` (`do-partition.ts:1514-1515`).

The transactional path does not. `groupItemsByRouting` puts every `"reject"` item into `unplaceable`
(`do-partition.ts:1566-1568`), and all four transaction entry points then assert:

```ts
invariant(unplaceable.length === 0, "fokos/partition.prepare: mis-routed item this node can neither own nor route");
```

(`do-partition.ts:1159`, `:1188`, `:1246`). So an overloaded — but perfectly healthy — partition makes
every transaction touching it die with a message that says the router is broken. On the `txCommit`
path this crashes a transaction that has already been decided.

(The original text also named `txCancel` as a fourth site. That was wrong: `txCancel` fans out to
child contexts from `splitStatus` and the promotion manager and never calls `groupItemsByRouting`.)

**Fix.** `shouldAllow` no longer returns a single `"reject"`. Its return type is now
`RoutingDecision = "ok" | "forward" | "reject_over_size" | "reject_out_of_range"`, and the two rejects
are handled differently everywhere:

| Decision | Handling | Retryable |
|---|---|---|
| `reject_over_size` | `overSizeError(op)` — `fokos/partition: partition exceeded its limits, please retry later (op).` | yes |
| `reject_out_of_range` | `invariant(false, misRoutedMessage(op))` | no |

Both routing sites now agree. `groupItemsByRouting` raises these itself instead of returning an
`unplaceable` array, which deleted the three duplicated `invariant(unplaceable.length === 0, …)` lines
at the transaction entry points; it takes the operation name so each still names its own path.

**Retries stopped.** Backpressure is deterministic for the life of a transaction — the partition is
over its cap and a split will not land inside a budget of a few seconds — so both `txPrepare` retry
loops in the coordinator now bail out immediately:

```ts
(err, nextAttempt) => !isPartitionOverSizeError(err) && nextAttempt <= 3   // do-transaction-coordinator.ts:312
(err, nextAttempt) => !isPartitionOverSizeError(err) && nextAttempt <= 5   // :497
```

That removes up to ~6 s and ~8 s of pointless backoff before the same cancellation. The other
`tryWhile` sites are left alone: the commit and cancel loops cannot see a size reject any more, and the
`txReadForTransaction` loops use `intent: "read"`.

`isPartitionOverSizeError` matches a substring, because Durable Object RPC carries only an error's
message across the boundary — not its class, not custom properties. `do-partition.test.ts` asserts the
predicate against a genuinely remote error, so the cross-RPC assumption is tested rather than assumed.

**Behaviour change to note.** On the NON-transactional path an out-of-range sort key previously
reported `partition exceeded its limits` too; it now reports mis-routing. That is the point of the
change — the message names the real cause — and `partition-topology.test.ts` was updated to assert it.

**Still open.** A size-rejected prepare still reaches the client as `RejectionReason
{ type: "transient_error" }`, so the cancelled transaction does not say it was backpressure even
though the partition-side message now does. Surfacing it would need a new `RejectionReason` variant,
which is a public API change — folded into the A-series work.

### B4. `transactGetItems` has a 1-millisecond ABA window, and ignores the exact version it already has — MEDIUM — ✅ FIXED

Fixed: the phase-1/phase-2 check now compares a `concurrencyStamp` built from the item's `version`
(primary, monotonic) plus `lastCommittedTs` (secondary, so a delete+recreate landing back on the same
version is still caught when the timestamps differ). See A4 for the result-shape half of the change.
A regression test in `transaction-participant.test.ts` writes the same item twice with an identical
`lastTransactionTs` and asserts that only `version` moves — the exact pair the old datum could not
distinguish. The description below is kept as the record of the defect.

---

The two-phase read compares `lastCommittedTs` (`do-transaction-coordinator.ts:569-576`).
`lastCommittedTs` is `last_transaction_ts`, which for a non-transactional write is `Date.now()` in
milliseconds (`do-partition.ts:409`).

So two non-transactional writes inside the same millisecond produce the same `lastCommittedTs`, and
phase 2 sees no change. The read returns phase 1's data and reports `committed`, although the value
changed under it.

The item already carries a strictly monotonic `v` (`partition/partition-store.ts:371`), which has no
such ambiguity, and the participant already reads the row (`partition/transaction-participant.ts:215`).

**Fix.** Return `v` from `readForTransactionLocal` and compare `(v, lastCommittedTs)` — or just `v` for
found items and `lastCommittedTs` for absent ones. This also fixes A4 for free.

This is what the ATC 2023 paper specifies for read transactions (§3.4). It does **not** compare
timestamps between the two phases; it compares a monotonic counter: *"the storage node not only returns
the item's value but also its current committed log sequence number (LSN) [...] The LSN increases
monotonically. In the second phase, the items are read again. If there have been no changes to the
items between the two phases, namely the LSNs have not changed, then the read transaction returns
successfully."* Our `v` column is the LSN analogue, so the fix aligns us with the paper rather than
inventing something.

Note the remaining, narrower hole: an item that is created **and** deleted between the two phases reads
as `found:false, lastCommittedTs:0` both times. This is inherent to the partition-level delete
watermark, which is the paper's deliberate trade (it avoids per-item tombstones), so it is a known cost
rather than a defect.

### B5. A "collision-proof" key identity that is not collision-proof — MEDIUM — ✅ FIXED

Fixed in the coordinator, and better than planned: rather than re-encoding the decoded keys to call
`KeyCodec.pairKey`, the read RPC no longer decodes them at all. `ReadForTransactionItemResultEncoded`
now carries `hashKey`/`sortKey` as canonical `KeyBytes` (sortKey `[]` = absent) exactly like the
request side, `db.ts` decodes once at the public exit alongside the json parse and the internals
strip, and `initiateRead` pairs the two phases with `KeyCodec.pairKey(r.hashKey, r.sortKey)` directly.
That restores the KeyCodec contract on an internal hop ("encode at entry, decode at exit, compare
bytes in between") and removes a decode-then-re-encode round trip per item per phase.

The same-pass cleanup replaced the `concurrencyStamp` string building with `sameCommittedState(a, b)`,
a direct field comparison — no per-item string allocation for the conflict check either.

The second site, `validateTransactWriteOperations`, is fixed too, without encoding anything twice: it
now takes the caller's `KeyEncoders` (the same injection pattern as `normalizeSkInterval(sort,
encodeSortKey)`, so the byte-size caps stay owned by `db.ts`), encodes each key exactly ONCE, uses
`KeyCodec.pairKey` for duplicate detection, and returns the canonical bytes in input order.
`transactWriteItems` consumes those bytes instead of running its own second encode pass — so the fix
removes two encodes per operation rather than adding any. The error messages also stopped
re-encoding the key just to render it.

A regression test covers the exact conflated pair (string sortKey `"9,9"` vs binary `[9,9]`); against
the old identity it fails with `duplicate key ("a", b64:_wkJ)` — a message that prints the two keys
as visibly different while rejecting them as the same.

Two places build a Map/Set key from raw public keys:

```ts
// transaction-limits.ts:76-77 — "Collision-proof composite identity for arbitrary key bytes."
const key = `${op.hashKey.length}:${op.hashKey}:${op.sortKey ?? ""}`;
```

```ts
// do-transaction-coordinator.ts:567-569 — "Re-encode ... to a collision-proof composite identity"
const resultKey = (r) => `${r.hashKey.length}:${r.hashKey}:${r.sortKey ?? ""}`;
```

Template interpolation of a `Uint8Array` produces a comma-joined decimal list, and the sort key has no
length prefix. So the string sort key `"9,9"` and the binary sort key `Uint8Array([9,9])` produce the
same identity, although `KeyCodec` treats them as different keys (the binary one carries a `0xFF` tag).

Consequences: `validateTransactWriteOperations` rejects a legitimate transaction as a duplicate; the
coordinator's phase1/phase2 matcher can pair the wrong two items and either miss a conflict or invent
one.

The correct primitive already exists and is already used by `commitLocal`
(`partition/transaction-participant.ts:151-152`): `KeyCodec.pairKey(hk, sk)` over encoded bytes
(`partition-topology/key-codec.ts:168-172`). Both call sites should use it. In the coordinator this
means keeping the encoded keys alongside the decoded ones, or letting the participant echo a stable id.

### B6. The coordinator never deletes anything — MEDIUM

There is no `DELETE` against `tc_state`, `tc_items` or `tc_participants` anywhere in
`do-transaction-coordinator.ts`. `tc_items` stores the **full item payload** of every transaction
(`:171-182`). So each coordinator DO keeps a permanent copy of every byte ever written through a
transaction, for the lifetime of the table.

Two follow-on costs:

- `alarm()` runs `SELECT ... WHERE state NOT IN ('COMMITTED','CANCELLED') LIMIT 100` and then
  `SELECT COUNT(*) ... WHERE state NOT IN (...)` (`:582-627`). There is no index on `state`, so both
  are full scans of a table that only grows. The alarm gets slower forever.
- The idempotency window is unbounded. DynamoDB's is 10 minutes.

**Fix.** Delete `tc_items` and `tc_participants` rows on the terminal transition, keep a compact
`tc_state` row (outcome + reason only) for the idempotency window, and delete it after a TTL. Add an
index on `(state, created_at)` for the alarm scan.

**Hard constraint on that GC, added 2026-08-16.** A `tc_state` row may be deleted ONLY in a terminal
state (`COMMITTED` / `CANCELLED`). `recoverTransaction` answers `not_found` for a missing row, and the
participant's stale-recovery job reads `not_found` as **cancel** (`do-partition.ts`, the
`CANCELLED || not_found` branch). Deleting a `COMMITTING` or `PREPARED` row would therefore make a
lagging participant cancel a transaction the others already applied — a torn transaction, silently.
Age is not a substitute for state: `COMMITTING` is exactly the state a slow participant leaves behind.
The terminal states are safe by construction, because `COMMITTED` is reached only once every
`tc_participants` row has `commit_outcome` set, and that is written after the `txCommit` RPC returned,
which awaited the entry DO's local commit and all its forwarded children — so no lock survives.

Two lesser points for the same work:

- Retain the compact row for at least `max(idempotency window, PartitionDO.STALE_TX_MS + slack)`, so a
  lagging participant reads the REAL outcome instead of falling through to the `not_found` inference.
  The inference is the backstop, not the mechanism.
- `runCancel` reads `tc_items` for its routing keys (5.1), so the `tc_items` delete must stay on the
  terminal transition, never before it.

### B7. `destroy()` leaves the coordinators intact — MEDIUM — ✅ FIXED

`TransactionCoordinatorDO.destroyCoordinator()` mirrors `PartitionDO.destroyPartition()`:
`deleteAlarm` (so the recovery alarm cannot fire on a wiped instance), `deleteAll`, then
`ctx.abort(DESTROY_ABORT_SENTINEL)` to evict the instance — `deleteAll` drops the tables AND the
migration bookkeeping, so without the eviction the cached `#migrations` would still believe the schema
exists.

`FokosDB.destroy` now sweeps every shard with `StaticShardedDO.all` **before** traversing the
partitions. Order matters: a transaction in flight is driven BY a coordinator, so wiping the
coordinators stops the drivers before the data goes; the reverse lets a live coordinator commit into a
partition the traversal has already emptied. All `numTransactionCoordinators` shards are swept, not
only the ones holding rows — the shard for a given idempotency token is not knowable from `db.ts`, and
an empty shard costs one wipe of empty storage.

The sentinel string was a magic literal in two files and is now `DESTROY_ABORT_SENTINEL` +
`isDestroyAbortError()` in `cf-utils.ts`, used by both DOs and both catch sites.

**Testing.** `test/destroy.test.ts` is skipped — a real `ctx.abort()` hangs the workers pool, and this
was re-confirmed (the unskipped test was killed at 180 s). So the coverage is at the DO level in
`do-transaction-coordinator.test.ts`, with `ctx.abort` stubbed by a spy: it seeds a `COMMITTED` row plus
items and an alarm, calls `destroyCoordinator()`, and asserts the abort happened with the sentinel, that
no `tc_*` or migration table survives, and that no alarm remains. Note the alarm assertion does not pin
`deleteAlarm` on its own — Miniflare's `deleteAll` clears the alarm too — it pins the end state.

The description below is kept as the record.

---

`FokosDB.destroy` traversed partitions only (`db.ts:357-382`). The `TransactionCoordinatorDO` shards
for that table kept all their `tc_state` rows.

So after a destroy and re-create of the same `tableName`, a client that replays an old
`clientRequestToken` gets the **old transaction's committed result** for data that no longer exists.

### B8. Idempotency-token replay with different operations is silently wrong — MEDIUM — ✅ FIXED

`initiateWrite` looks up the token and, if found, returns the stored outcome without ever comparing the
new operations against `tc_items` (`do-transaction-coordinator.ts:143-146`). A client that reuses a
token for different work gets "committed" for writes that never happened.

DynamoDB raises `IdempotentParameterMismatch` here. At minimum, store a hash of the normalised
operation set and reject a mismatch.

**Fix.** `tc_state` gained a `operations_hash TEXT NOT NULL` column. `initiateWrite` computes the
fingerprint once (`transaction-idempotency.ts`) and uses it twice: to validate a replay, and as the
value stored with the new row. A replay whose fingerprint differs throws instead of resuming.

Hashing detail, since the payload can be 4 MB:

- xxHash64 via the existing `hash64` (WASM, synchronous). NOT `crypto.subtle`, which is async and has
  no streaming digest in Workers — it would force concatenating every payload into one buffer.
- Fields are chained through xxhash's **seed** (`h = hash64(field, h)`) instead of being concatenated,
  so `data` is hashed where it already lives: no copy, no second pass.
- The fold across operations is a wrapping ADD, so it is commutative — the same items in a different
  order replay cleanly, without allocating a sorted copy to get that property.
- A per-operation token carries the operation, the kind, and WHICH optional fields are present, so
  `data: ""` cannot fingerprint the same as no data, and text `"5"` cannot match the byte `0x35`.
- A final chain through `ops.length` avalanches the sum and pins the set size.
- Stored as hex TEXT because Durable Object SQL cannot bind a JS bigint (`TypeError: Cannot convert a
  BigInt value to a number`, verified) and `Number()` would lose precision above 2^53.

Not cryptographic, deliberately: this catches a client mistake, and a crafted collision would only
mislead the client that crafted it.

The column was added to the migration-1 `CREATE TABLE` rather than as an `ALTER TABLE`, since the
project is pre-release and breaking the on-disk format is currently allowed.

### B9. `walkRangeChildren` emits a cursor for a fully-drained scan — LOW — ✅ FIXED

Fixed for BOTH budget exits, not only the byte one named below. The children that can contribute to
the page — they intersect the interval and are not entirely behind the resume cursor — are now selected
into `candidates` before the loop instead of being `continue`d inside it, which turns "could a later
child still contribute?" into `i < candidates.length - 1`. Both exits ask it before emitting a cursor.

The visit-cap exit had the same defect in a subtler form: it asked `i < orderedChildren.length - 1`,
counting children the interval excludes. So a query bounded to the first two leaves of a four-leaf
router, with the visit cap spent by exactly those two, emitted a boundary cursor into a range holding
nothing for it.

Two regression tests in `do-partition.test.ts`, both confirmed to fail against the old code:

- The byte exit is driven deterministically by replaying a full scan's own `bytesConsumed` as the
  budget. Every leaf still drains itself, and the router's remaining bytes reach zero exactly as the
  last leaf finishes — the one case where "budget exhausted" does not mean "more rows exist".
- The visit exit uses an exclusive upper bound at the third child's start boundary with
  `maxPartitionVisits: 2`.

`remainingLimit` cannot drive the byte-exit case: a leaf that stops on `maxItems` returns a cursor of
its own (`batch-scan.ts:70`), so the walk takes the earlier branch.

The description below is kept as the record.

---

When the byte budget hit zero on the last intersecting child and that child returned
`nextCursor === null`, the router still synthesised a cursor from the last item
(`do-partition.ts:696-700`). The client then made one more round trip that returned zero items.

`db.ts` handles the same situation correctly: it only emits a cursor when a **later** non-empty
sub-query exists (`db.ts:323-344`). The two loops implement the same budget protocol and should agree.

### B10. `PageBudget.budgetExhausted` tests `=== 0` — LOW — ✅ FIXED

```ts
get budgetExhausted(): boolean { return this.remainingBytes <= 0 || this.remainingLimit === 0; }
```

(`query/page-budget.ts:26`). The byte side uses `<= 0`; the limit side uses `=== 0`. Today no caller
can overshoot, so it works — but a single child that returns one item too many makes the limit go
negative and the budget silently reports "not exhausted" forever.

**Fix.** Not a bare `<= 0`, which would be a worse bug: `remainingLimit` is `number | null`, and
`null <= 0` is `true` in JS because relational comparison coerces null to 0 — so every UNLIMITED query
would report exhausted before reading an item. `=== 0` was accidentally safe against that. The fix
keeps the null case explicit:

```ts
return this.remainingBytes <= 0 || (this.remainingLimit !== null && this.remainingLimit <= 0);
```

`query/page-budget.test.ts` covers both hazards: reverting to `=== 0` fails the overshoot test, and
substituting a bare `<= 0` fails the null-limit test.

Why the overshoot is unreachable today: both `consume` callers (`db.ts:332`, `do-partition.ts:698`)
pass `remainingLimit` down, and the leaf caps its scan with `maxItems: remainingLimit ?? undefined`
(`do-partition.ts:614`). The guard now holds on its own instead of depending on that contract.

### B11. Size backpressure rejects reads — LOW — ✅ FIXED

`apiGetItem` and `apiQueryItems` go through `withSplitForwarding`, which honours the `"reject"`
decision (`do-partition.ts:1514`). A read cannot grow the partition, so refusing reads when a partition
is 10% over its split threshold removes availability with no benefit. Backpressure should apply to
writes only.

Two other paths turn out to be non-growing for the same reason:

- **`deleteItem`** only runs `DELETE FROM items` and UPDATEs already-existing rows
  (`deletion_metadata` at fixed `id = 1`, `key_size_estimates` for an existing `hk`). No INSERT on any
  path. It is also the one operation that brings an over-size partition back under its cap, so
  rejecting it leaves the partition stuck.
- **`txCommit`** cannot grow the partition either. `prepareLocal` persists the full payload —
  `data`, `kind`, `conditions_json` — into `pending_transactions`
  (`partition/transaction-participant.ts:122-136`), so the size high-water mark is at **prepare**.
  Commit moves those bytes into `items` and then drops the pending row, so post-prepare
  `items(old) + pending(new)` becomes `items(new)`: a delta of `−old`. Refusing it would wedge a
  transaction the coordinator has already decided, for bytes that are already on disk.

`txPrepare` stays a genuine write: it inserts the pending rows, and rejecting it is safe because the
coordinator then cancels — a clean outcome the client can act on.

**Fix:** `shouldAllow` takes a third argument, `intent: OperationIntent`, and both implementations
gate ONLY their size check on `intent === "write"`:

| Path | Intent |
|---|---|
| `apiPutItem`, `txPrepare` | `write` |
| `apiDeleteItem` | `delete` |
| `apiGetItem`, `apiQueryItems`, `txReadForTransaction` | `read` |
| `txCommit` | `ignore_size_reject` |

The three non-growing values take the same branch today. They stay distinct so a call site states what
it is doing rather than pre-computing the policy's answer.

Routing is untouched. `"forward"` and the range partition's out-of-range `"reject"` are correctness,
not load, so they still reject **every** intent — `ignore_size_reject` names the size reject only and
buys nothing past a mis-route.

New tests in `src/lib/partition-topology/split-policy.test.ts` build both topologies over real DO
storage with a cap small enough that an empty database is already over it, and assert that an
over-size partition rejects `write` but serves every non-growing intent, that the out-of-range
rejection survives for every intent, and that an under-size partition allows everything. The intent
lists are written out rather than derived, so a new `OperationIntent` value forces a decision about
which side of the gate it belongs on.

**Effect on B3:** the commit path is no longer reachable by size backpressure, so B3's remaining scope
is a size-rejected `txPrepare` surfacing as `invariant_failed: mis-routed item this node can neither
own nor route` instead of a clean retryable error.

---

## 5. Smaller items

Numbered S1-S9 so they can be referenced. The numbers follow document order and do not change when
an item is closed.

- ~~**S1.** `db.ts` `encodeItemData` accepts a JSON primitive (`5`, `true`, `null`) at runtime, but
  `JsonComposite` (`types.ts`) says composites only.~~ **DONE** — settled by REJECTING, not widening:
  the type's own comment ("start restricted; top-level primitives excluded initially") makes the
  restriction deliberate, so the runtime was aligned to the type. `encodeItemData` now throws
  `data must be an object, array, string or Uint8Array (got number)` for anything that is not an
  object. Relaxing this later is not breaking; widening now and re-restricting later would be.
  Only a JS caller can reach it — TypeScript rejects a primitive, and the HTTP surface types `data` as
  a string — which is exactly why it needed a runtime check. `transactWriteItems` shares
  `encodeItemData`, so both write APIs reject the same values (the A9 property).
- ~~**S2.** `db.ts` — `JSON.stringify` returning `undefined` produces the message
  `"data serialized to undefined"`.~~ **DONE.** S1's guard removes every top-level cause (function,
  symbol, `undefined`), leaving exactly one: a `toJSON()` that itself returns undefined, which makes
  the WHOLE document undefined rather than one field. The message now names that
  — `data is not JSON-serializable (its toJSON() returned undefined)` — and the `catch` branch quotes
  the `JSON.stringify` error, so a circular reference and a BigInt no longer report identically.
- **S3.** `itemDataBytes` (`transaction-limits.ts`) measures a string as `data.length`, which is UTF-16
  code units. The store's `est_row_bytes` generated column uses `octet_length`, which is UTF-8 bytes.
  So for any non-ASCII text the JS cap UNDER-counts what is actually stored — by 3x for CJK, 2x for
  emoji — and both `putItem`'s `MAX_ITEM_BYTES` check and the transaction's `MAX_PAYLOAD_BYTES` total
  admit items larger than they intend. The function's own `FIXME` asks for the same fix plus counting
  keys into the item budget, per DynamoDB. (The original text of this entry described a `length * 2`
  UTF-16 double-count; A9 rewrote that code, and the surviving defect is the one above.)
- ~~**S4.** `validateTransactWriteOperations` does not reject `data` on a `delete` or `check` operation.
  Such data is stored in `tc_items` and `pending_transactions` and then ignored.~~ **DONE** with A7.
- **S5.** `types.ts` still carries the `FIXME` about `QueryItemsResult.data` versus the HTTP wire type.
  Resolving A2 resolves that FIXME.
- **S6.** `do-transaction-coordinator.ts` — `TODO: append DO shard suffix for tie-breaking`. This is the
  same item as the README's "Optimize the transaction timestamp/numbering to reduce conflicts". Note
  that until it lands, two coordinators on the same millisecond produce a genuine tie, and the
  `<=` comparison in `prepareLocal` (`transaction-participant.ts`) makes one of them fail rather
  than break the tie.
- ~~**S7.** `do-partition.ts` — the `txCancel` fan-out broadcasts to every promoted range root, not only
  to the ones that hold locks for this transaction.~~ **DONE** — option B, see 5.1.
- **S8.** `groupItemsByRouting` (`do-partition.ts`) does not consult the `PartialRangeTopology` bloom
  filter that `withSplitForwarding` uses as its step 2. Both check the authoritative `PromotionManager`
  first; only the read/write path then makes the speculative hop for a key a DESCENDANT promoted. So a
  transaction takes more hops than a plain read for the same key. Marked `FIXME`; the note there is
  that a bloom false positive needs care in a multi-item flow.
- **S9.** `partition/partition-store.ts` — `range_hierarchy` has no size bound or cleanup policy, and it
  is written on every forwarded request. Marked `FIXME`; it is a slow, unbounded growth path.

### 5.1 `txCancel` fan-out — the options — ✅ FIXED (option B)

Written up 2026-08-10, implemented 2026-08-16 as **option B**. What landed:

- `CancelRequest` is `{ transactionId, items: TransactionItemKey[] }`. `TransactionItemKey` is the new
  name for the key half of a wire-IN item (`{ hashKey: KeyBytes; sortKey: KeyBytes }`), which
  `TransactionItem` and `ReadForTransactionRequest` now both build on instead of restating.
  Keys only — a cancel never carries a payload.
- `txCancel` keeps `cancelLocal(transactionId)` first, then routes with
  `groupItemsByRouting(items, "ignore_size_reject", "cancel")`. Same intent as `txCommit`: cancel only
  DELETEs, and it is the operation that brings an over-size partition back under its cap, so
  backpressure must not wedge it. Both broadcast sources — the `splitStatus` children and
  `activeRangeRootHashKeys()` — are gone.
- `runCancel` groups by partition exactly like `runCommit`, from a new `loadItemKeys` that selects
  only `hk, sk, partition_do_name`. `loadItems` would have pulled up to `MAX_PAYLOAD_BYTES` of item
  data on the one path that never looks at it, and cancels are the common case under contention.
  It still serves a NULL-`prepare_outcome` participant, because `tc_items` is written before prepare.
- The stale-recovery job hoists its `listPendingTxItems` read above the branch, so the cancel half
  routes on this node's own locked keys — the same source the commit half already used.
- `PromotionManager.activeRangeRootHashKeys()` is deleted. Its doc comment named the cancel fan-out as
  its reason to exist, and it had no other production caller. The two test assertions on it duplicated
  the `statusFor` / `getPromotedKeyStatus` lines directly above them.

**Regression coverage.** `do-partition.test.ts`, "cancel via hash DO releases both local and
promoted-key locks", was already the right test and now proves routing rather than broadcast: alice is
promoted so its lock sits on the range root, bob's is local, and the re-prepare that follows only
succeeds if both were released. Confirmed by mutation — routing the empty list instead of
`request.items` makes it fail with `expected 'rejected' to be 'accepted'`.

**Not fixed here**, and still true: a keyless cancel (`items: []`) is legal and means local-only, which
is option D. Nothing in the tree sends one today; it is the acknowledged fallback, written on the type.

The options below are kept as the decision record.

**The frame.** One fact decides the shape of every option: a lock is always released eventually by the
node that holds it. `prepareLocal` arms a stale alarm on accept (`do-partition.ts:1220`), the
background loop re-arms while any lock remains (`do-partition.ts:1880`), and each holder asks its TC
directly (`recoverTransaction`) and cancels itself. A split carries the locks AND their
`coordinator_do_id` into the child (`partition/migration.ts:105`), so the self-healing follows the data.

So the fan-out is a **latency** mechanism, not a correctness one. Its job is to release locks in
milliseconds instead of `STALE_TX_MS`, which matters because a held lock makes non-transactional writes
to that key throw — under contention, exactly the keys everyone wants. Each option is judged on whether
it reaches every holder PROMPTLY, and on what it costs per cancel.

**Why cancel cannot route today.** `CancelRequest` is `{ transactionId }` and nothing else
(`transaction-types.ts:71`), while `PrepareRequest` and `CommitRequest` both carry `items` and route
with `groupItemsByRouting`. With no keys there is nothing to route on, so `txCancel` sweeps: split
children from `splitStatus`, plus EVERY key in `activeRangeRootHashKeys()`
(`partition/hash-key-promotion.ts:78`) — the partition's whole promoted set, unrelated to the
transaction — and each of those repeats the pattern down its own subtree.

**A. Persist the forward targets at prepare.** `txPrepare` already computes the exact set of downstream
DOs (`groupItemsByRouting`'s `forwarded` map, `do-partition.ts:1572`). Write
`(transaction_id, do_name, pCtx)` rows, have cancel read them back, delete on commit/cancel.

- Coverage: exact, and works with zero keys, so both callers are served identically.
- Cost: one INSERT per distinct forwarded child per prepare (not per item), plus a new table.
- The catch: `split_completed` drops the parent's whole `pending_transactions` (`do-partition.ts:978`)
  while a keyless cancel can still arrive at the parent afterwards. The forward log must then be
  REWRITTEN to point at the children rather than deleted with the locks — a new invariant in the
  migration path, and the real cost of this option.

**B. The TC sends the keys.** `CancelRequest` becomes `{ transactionId, keys }` and `txCancel` routes
with `groupItemsByRouting` exactly like `txCommit`.

- In the TC this is three lines: `runCancel` (`do-transaction-coordinator.ts:423`) is `runCommit`
  (`:369`) minus `loadItems` + `groupByPartition`, which already exist. It keeps working for a
  participant with a NULL `prepare_outcome`, since `tc_items` is written before prepare.
- In the partition, the recovery job takes its keys from `listPendingTxItems` — the same source the
  COMMIT branch of that same job already uses (`do-partition.ts:1796-1810`).
- Coverage: exact for the TC path. Routing is a pure function of the key; locks follow their key
  through a split; a promotion cutover cannot happen while a key is locked and a promoted root never
  inherits locks (`do-partition.ts:893-901`). So no lock is left anywhere routing does not point.
- `cancelLocal(transactionId)` deletes by transaction id, not by key (`do-partition.ts:1249`), so every
  node the cancel PASSES THROUGH is still fully cleared — including a parent mid-split, which is the
  routing entry point. Key-routing does not open a split-window gap.
- Cost: zero extra writes, no new state, no new invariant.

**C. Narrow only the promoted-root broadcast.** Keep cancel keyless, but at prepare record just the
promoted hash keys forwarded to a range root. Kills the unbounded part and leaves the split-child
broadcast, which is bounded by `splitN` and points at nodes that plausibly hold locks. Smaller than A,
but still a new table and still needs a migration story.

**D. Local-only cancel, no fan-out.** Correct, thanks to self-healing, but every descendant lock lingers
for `STALE_TX_MS`. Not acceptable alone — it degrades precisely the contended case. It is the right
fallback UNDERNEATH B.

| Option | DOs woken per cancel at a hash partition | Extra writes | New state |
|---|---|---|---|
| today | 1 + K promoted roots + split children, recursively | — | — |
| A | exactly the DOs prepare touched | 1 per forwarded child per prepare | forward-log table + split rewrite rule |
| B | exactly the DOs owning this transaction's keys (usually 1) | none | none |
| C | 1 + split children | 1 per promoted forward | small table |
| D | 1 | none | none |

**Recommendation: B, with D as the acknowledged fallback.** It removes the unbounded fan-out entirely,
makes cancel symmetric with commit at both layers instead of a special case, and costs nothing in
storage or new invariants. The residual is that a RECOVERY-initiated cancel clears only the initiating
node and leaves descendants to their own alarms — which is already the behaviour today whenever prepare
forwarded every item, since the ancestor then holds no row for that transaction and never wakes for it.

A is worth doing only to keep cancel keyless as a design property. That property buys little: both
callers can name their keys, and the migration bookkeeping it demands is the one genuinely error-prone
piece in this area.

---

## 6. Proposed unified API

The concrete shape I recommend, to close A1–A7 together. Keys, `data`, and `conditions` mean exactly
what they mean today.

**One item envelope, everywhere.**

```ts
type FokosItem = {
  hashKey: string | Uint8Array;
  sortKey?: string | Uint8Array;
  data: string | Uint8Array | JsonValue;
  kind: DataKind;
  version: number;              // `v` — also returned by transactGetItems
  ttlEpochUTCSeconds?: number;
};

type ItemRead = { found: true; item: FokosItem } | { found: false; key: ItemKey };
```

`getItem`, `queryItems` and `transactGetItems` all return `ItemRead`. `putItem` and `deleteItem` return
`{ key, version, meta }` and `{ key, deleted, meta }`.

**One failure model.** Every operation throws a typed error carrying the existing `RejectionReason`
union. `transactWriteItems` keeps its `{ outcome }` return only if you prefer values over exceptions —
in which case `putItem`/`deleteItem` should return `{ outcome }` too. Do not keep both.

**One metadata block.** `meta: OperationMetrics & PartitionInfo` on every result, transactions
included. The `_internal` half is DONE (A11): `PartitionInfo` is public-clean and the RPC types carry
`PartitionInfoInternal`. What A3 still owes is the meta on the two transaction methods.

**One request vocabulary.** DONE (A7), except that `queries` kept its name — see A7 for the rule
applied instead.

```ts
transactWriteItems({ items: TransactWriteItem[], clientRequestToken?, ttl? })
transactGetItems({ items: ItemKey[] })          // limited to MAX_ITEMS_PER_TRANSACTION
queryItems({ queries: QuerySpec[], limit?, maxPageBytes?, cursor? })
```

`ttl?` on `transactWriteItems` is still owed by A8.

**Ordering guarantee.** `transactGetItems` returns results positionally matched to the request — DONE
(A6): `initiateRead` re-orders by walking the request. `transactWriteItems` returns no items (A5), so
the guarantee it still owes is on the failure path: a positional `CancellationReasons` equivalent.

**Shared limits.** `MAX_ITEMS_PER_TRANSACTION` applies to reads as well as writes.
`MAX_PAYLOAD_BYTES` (or a per-item equivalent) applies to `putItem` as well.

---

## 7. Suggested order of work

1. ~~**B1** (`MAX` on `last_transaction_ts`) — one line, prevents a lost update.~~ **DONE.**
2. ~~**B2** (answer from the decision, not from the cleanup) — the correct 2PC semantics.~~ **DONE.**
3. ~~**B3** (separate backpressure from mis-routing) — small, stops a crash on a decided transaction.~~
   **DONE**, except that a size-rejected prepare still reports `transient_error` — folded into A1.
4. **A8** (TTL) — fix the `&&` guard, reject or implement `ttlSeconds`, and carry TTL through
   `TransactionItem` so a transactional put stops clearing it.
5. ~~**B4** (compare `v`) + **A4** (return `version`, drop the 2PC internals) + **B5**
   (`KeyCodec.pairKey` everywhere, on KeyBytes carried end-to-end).~~ **DONE.**

   That work also settled a layering rule worth keeping: **the TC never decodes keys.** Both TC
   responses (`InitiateWriteResponseEncoded`, `ReadForTransactionItemResultEncoded`) now carry
   canonical `KeyBytes` with `[]` as the absent sortKey, matching the request side and the `tc_items`
   BLOB columns, and `db.ts` decodes at the single public exit. `RejectionReason` is the one
   deliberate exception — it is persisted in `tc_state.rejection_reason_json` and replayed verbatim on
   an idempotent retry, so it stays in public form (bytes there would need the same `$u8` JSON tagging
   plus a decode on every replay). This is documented on the type.
6. ~~**A11** (strip `_internal` at the `db.ts` boundary).~~ **DONE** — split the type as well, so a
   new public exit that forgets the strip is a type-level mistake.
7. **B6** (coordinator GC) — before anyone runs this long enough to notice. ~~**B7** (destroy sweep).~~ **DONE.**
8. **A1–A7** as one deliberate breaking change, guided by Section 6. **A7 is DONE.** This is the right moment: the
   README already says breaking changes are expected, and every item above is cheaper to do once the
   envelopes agree.

Deferred, not forgotten: the packed `(ms, tiebreak)` timestamp encoding discussed alongside B1. It
needs a schema migration across `last_transaction_ts`, `transaction_ts` and `max_deleted_ts`, and it
buys throughput (fewer same-millisecond false conflicts, plus the coordinator tie-break the README
asks for), not correctness.
