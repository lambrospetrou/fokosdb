# Implementation Plan: `KeyCodec` + canonical UTF-8 `KeyBytes`

**Read first:** `docs/ideas/key-ordering-utf8-convergence.md` — the SPEC. It owns the *why* and all the
*decisions* (byte ordering, `0xFF`-tag binary, `[]` for absent key, reject empty/lone-surrogate, BLOB
schema, depth-seeded hashing, DO-name percent-encoding, never-compare-raw invariant). This document is
the *how*: the module shape, the call-site cutover, and the milestone order. It does **not** repeat the
SPEC's rationale — when a choice looks arbitrary here, the reason is in the SPEC.

This plan is written against the **current** tree (the SPEC's file:line references are stale; the code
moved to `src/lib/partition-topology/`). All file:line references below are current as of this writing.

---

## Global conventions (apply in every milestone)

- **`KeyBytes`** — a branded `Uint8Array` (SPEC *Enforcement*). The **only** producer is
  `KeyCodec.encode`. No other code fabricates one. Comparisons/synthesis on keys go **only** through
  `KeyCodec`. There is no `<`/`charCodeAt`/`.substring` on keys anywhere else.
- **Encode at entry, decode at exit, bytes in between.** Public keys (`string | Uint8Array`) are encoded
  at the `FokosDB` / DO-RPC boundary; everything inward (routing, ownership, store, cursors, boundaries)
  is `KeyBytes`; only result-producing exits (`queryItems`, transaction reads) `decode`. `getItem`
  echoes the caller's input key (no decode needed).
- **RPC erases the brand** (SPEC sharp-edge #4). At each DO-RPC entry that receives keys, re-brand the
  incoming `Uint8Array` with a trusted `asKeyBytes()` and treat the payload as already-encoded.
- **Build stays green at every milestone.** Where a downstream layer hasn't been cut over yet, add a
  *temporary* encode/decode adapter at the boundary and delete it in the milestone that owns that layer.
  Each milestone lists its adapters and which later milestone removes them.

---

## The `KeyCodec` module (built in M0, used everywhere after)

New file: `src/lib/partition-topology/key-codec.ts` (co-located with routing; the store imports it).

```ts
declare const KEY_BRAND: unique symbol;
export type KeyBytes = Uint8Array & { readonly [KEY_BRAND]: true };

export const KeyCodec = {
  encode(key: string | Uint8Array): KeyBytes,          // string→raw UTF-8; binary→[0xFF, ...bytes]
  encodeOptional(key: string | Uint8Array | undefined): KeyBytes, // undefined→[] (the absent sentinel)
  decode(k: KeyBytes): string | Uint8Array,            // first byte 0xFF ⇒ binary; else UTF-8
  compare(a: KeyBytes, b: KeyBytes): number,           // unsigned memcmp
  successor(prefix: KeyBytes): KeyBytes | null,        // prefix upper bound; null = unbounded (all-0xFF)
  shortestSeparator(lo: KeyBytes, hi: KeyBytes): KeyBytes, // byte-space separator, lo < r <= hi
  asKeyBytes(bytes: Uint8Array): KeyBytes,             // trusted re-brand for RPC inputs (no copy/validate)
};
```

Behavioral notes (the non-obvious bits the implementer must get right):

- **`encode` validation.** Reject (throw) empty `string` and empty `Uint8Array`, and any string that is
  not well-formed UTF-16 (`str.isWellFormed?.() === false`, i.e. contains a lone surrogate) — SPEC
  decisions. Absent keys never reach `encode`; they come from `encodeOptional(undefined) → []`. So
  `encode` never returns `[]`.
- **String encoding** is `new TextEncoder().encode(s)` (raw UTF-8, no tag). Binary is `0xFF` then the
  raw bytes. UTF-8 never contains `0xFF`, so the tag is unambiguous and needs no escaping.
- **`compare`** is a plain unsigned byte loop then length tiebreak — it must produce the *same* total
  order as SQLite `BLOB`/`memcmp` (verified in M0).
- **`successor(prefix)`**: increment the last byte that is `< 0xFF` and drop everything after it; if all
  bytes are `0xFF`, return `null` (unbounded upper edge). Used by `beginsWith`/prefix upper bounds. Note
  the high end of the binary keyspace is genuinely `0xFF…`, so `successor` of a binary prefix is normal
  byte arithmetic; only the truly all-`0xFF` input is unbounded.
- **`shortestSeparator(lo, hi)`** (byte port of today's `partition-store.ts:64-74`): walk to the first
  differing byte `i`; return `hi[0..i+1]`. If `lo` is a proper prefix of `hi`, return `hi[0..lo.length+1]`.
  Pre-condition `compare(lo, hi) < 0`. Result `r` satisfies `lo < r <= hi` in byte order. This is the
  only synthesis primitive; it never special-cases the `0xFF` tag (the tag is just leading bytes).

**Allocation contract (keep the hot path copy-free):**

- **`asKeyBytes` is a zero-cost cast** — `return bytes as unknown as KeyBytes`. The brand is erased at
  runtime; no copy, no validation, no new buffer. **Never** `.slice()`/`Uint8Array.from()` inside it, or
  it silently becomes a copy. It is the trusted re-brand path (already-encoded buffers from RPC and
  BLOB reads); `encode` is the only path that does real work.
- **`TextEncoder`/`TextDecoder` are module-level singletons.** `.encode()`/`.decode()` are stateless —
  each call returns its own fresh buffer/string and holds no cross-call state — so a shared instance is
  safe across keys and saves a per-call allocation. (We do **not** use `encodeInto` or streaming
  `decode`, the only stateful modes.)
- **BLOB reads wrap, don't copy:** `new Uint8Array(arrayBuffer)` is a view over the existing buffer (a
  tiny wrapper object, no byte copy). If the DO SQL API returns a `Uint8Array` directly, skip even that.
- **The only real copies** are `encode(string)` (one `TextEncoder` alloc — once per key per request,
  ~cost-neutral vs today's hidden UTF-16→UTF-8 at the SQLite binding), `encode(Uint8Array)` (the `0xFF`
  lead forces one `length+1` buffer — the SPEC's "+1 byte for binary"; strings pay nothing), and the
  synthesis primitives `successor`/`shortestSeparator` (small results, split-only cold path). Routing,
  ownership, and store comparisons allocate nothing — they `memcmp` the carried `KeyBytes`.

**Acceptance (M0):** a unit-test suite for the module plus the SPEC's ground-truth cross-check:

- Round-trip: `decode(encode(k)) === k` for ASCII, BMP, astral, and binary fixtures; `encode` throws on
  `""`, `new Uint8Array(0)`, and a lone-surrogate string.
- Ordering: `encode("￿")` vs `encode("😀")` — assert `compare` gives `"￿" < "😀"` (the SPEC's
  canonical case), the opposite of JS `<`.
- **SQLite cross-check** (open-question #1): in a DO-backed test, insert the fixture keys as `BLOB`,
  `SELECT ... ORDER BY <col>`, and assert the row order equals sorting the same `KeyBytes` with
  `KeyCodec.compare`. Include astral, binary (`0xFF`-led), and empty `[]`. (`PRAGMA encoding` is not a
  dependency — BLOB memcmp is encoding/collation-independent; this test is the real ground truth.)
- `shortestSeparator`/`successor` property tests: `lo < r <= hi`; `successor` is the least key with the
  prefix as a strict lower bound; all-`0xFF` ⇒ `null`.

---

## Milestone map (each compiles, each has an isolated test surface)

| M | Scope | Primary files | Isolated test |
|---|-------|---------------|---------------|
| M0 | `KeyCodec` + ground truth | `key-codec.ts` (new) | new `key-codec.test.ts` + BLOB cross-check |
| M1 | Hash on bytes | `hash-primitives.ts`, `hash-topology.ts`, `router.ts` | hash distribution/determinism test |
| M2 | Store → BLOB | `partition/partition-store.ts` | `partition-store.test.ts` |
| M3 | Topology routing + boundaries + DO names | `split-policy.ts`, `partition-context.ts`, `partition-id.ts`, `partial-range-topology.ts` | split-policy + partition-id round-trip tests |
| M4 | DO + 2PC path | `do-partition.ts`, `partition/transaction-participant.ts`, `do-transaction-coordinator.ts`, `transaction-limits.ts` | `transaction-participant.test.ts`, `migration.test.ts` |
| M5 | Public API widening | `db.ts`, `types.ts`, `transaction-types.ts`, `transaction-limits.ts` | end-to-end binary-key test |
| M6 | Hardening | logging, lint rule, key-size limits, full matrix | full astral/binary/empty matrix |

---

## M0 — `KeyCodec` + ground truth

Build the module and its tests as specified above. **No wiring into the rest of the app yet** — this
milestone exists so the codec is fully proven in isolation before anything depends on it.

Also confirm the `js-xxhash` API here (it's needed in M1): check whether `xxHash32` accepts a
`Uint8Array` input and a numeric seed. If it only accepts `string`, add a tiny shim in `key-codec.ts`
(or a `hash-bytes.ts`) that hashes a `Uint8Array` with a seed — do **not** stringify bytes to hash them.

**Done when:** `key-codec.test.ts` is green including the BLOB `ORDER BY` cross-check; nothing else in
the tree imports `KeyCodec` yet.

---

## M1 — Hash routing on `KeyBytes` (depth-seeded)

Files: `partition-topology/hash-primitives.ts` (24 lines), callers `hash-topology.ts`
(`findLeaf`/`updateFromHint`, lines ~77/96), `router.ts` (`findPartition`, line ~80).

Changes (SPEC sharp-edge #1):
- `hashChildIndex(keyBytes: KeyBytes, parentAbsDepth, K)` and `hashRootIndex(keyBytes: KeyBytes, n)` take
  `KeyBytes` and hash the **same** bytes at every level. Replace the `hashKey + (depth+1)` **string
  concat** with a **depth-varied seed**: `seedForDepth(d) = (GOLDEN_RATIO ^ Math.imul(d, PRIME)) >>> 0`
  with `PRIME` odd. `hashRootIndex` uses `GOLDEN_RATIO` (depth 0). No per-level allocation.
- Callers pass the already-encoded `KeyBytes`. `findLeaf`/`updateFromHint`/`findPartition` signatures
  take `KeyBytes` for the hash key.

Adapter: callers above are reached from the router, which still takes `string` until M5 — encode at the
router's `pickPartition` boundary for now (temporary; M5 makes `pickPartition` accept encoded keys /
encode once at the `db.ts` entry).

**Test:** unit test that hashing is deterministic, that `seedForDepth` is a bijection over small depths
(no per-depth collisions), and that distribution across `K` is roughly uniform for a fixture key set.
Clean-slate ⇒ changed hash values are fine; only internal consistency matters.

---

## M2 — Store goes BLOB (`partition/partition-store.ts`)

This is the foundational data-layer cutover. After it, the DO database stores and compares keys as
canonical bytes.

**Schema (migrations 1–4, edited in place — clean slate, SPEC *Constraints*):**
- `items`: `hk BLOB NOT NULL`, `sk BLOB NOT NULL DEFAULT x''` (was `TEXT`/`DEFAULT ''`). PK `(hk, sk)`.
- `pending_transactions`: `hk`/`sk` → `BLOB`, `sk … DEFAULT x''`. PK `(hk, sk, transaction_id)`.
- `promoted_keys`: `hash_key BLOB NOT NULL PRIMARY KEY`.
- `key_size_estimates`: `hk BLOB NOT NULL PRIMARY KEY`.
- Leave non-key columns (`data ANY`, etc.) untouched. Keep `WITHOUT ROWID, STRICT`. Confirm `STRICT`
  allows `BLOB` columns (it does) and that binding a `Uint8Array` yields a SQLite BLOB (it does in
  workerd DO SQL).

**Row/cursor types** (lines ~23-46): `MigratedItem`, `PendingTransactionRow`, `PendingTransactionCursor`,
`MigrationCursor` — `hk`/`sk` become `KeyBytes`. Reading a `BLOB` column yields `ArrayBuffer`; wrap as
`KeyCodec.asKeyBytes(new Uint8Array(buf))` when materializing rows. SQL bind params take `KeyBytes`
directly (a `Uint8Array` binds as BLOB).

**SQL statements** (no logic change, just bytes instead of strings) — the inventory of statements to
re-check: `getItem`/`getItemStamp` (`WHERE hk=? AND sk=?`), `upsertItem` (`ON CONFLICT(hk,sk)`),
`deleteItem`, `insertItemIfAbsent`, `hasItemsForHashKey`, `queryItemsPage` (`WHERE hk>? OR (hk=? AND
sk>?) ORDER BY hk, sk`), `queryRangeItemsPage` (`sk >= ? / sk > ? / sk < ?`), the pending-tx family
(`insertPendingLock`, `pendingLockFor`, `listPendingTxKeys`, `getPendingTxOp`, `listPendingTxItems`,
`queryPendingTxPage`), `deleteItemsBatchForHashKey`, `rebuildKeySizeEstimates`. The byte ordering of
`ORDER BY`/`<`/`>=` on BLOB columns now matches `KeyCodec.compare` (proven in M0).

**Synthesis & comparison** (the bug epicentre):
- Delete the UTF-16 `shortestSeparator` (lines 64-74); call `KeyCodec.shortestSeparator`.
- `computeRangeSplitBoundaries(hashKey, start, end, N)` (lines 395-437): params and return become
  `KeyBytes`/`KeyBytes[]` (SPEC open-question #5). `lower = start ?? KeyCodec.encodeOptional(undefined)`
  i.e. `[]`. Boundaries synthesized via `KeyCodec.shortestSeparator(rows[i].sk, rows[i+1].sk)`. The
  strict-monotonic guard (`boundaries[i] <= lower`, `<= boundaries[i-1]`) uses `KeyCodec.compare`. The
  guard is now **consistent** with the SQL scans that migrate the data.

**Size estimators** (SPEC sharp-edge #6 — affects split/promotion *triggering*, not just reporting):
`estimateRowBytes`, `estimateItemBytes`, `estimatePendingTxBytes` (lines ~104-121) currently use
`.length` (UTF-16 units) and `*2`. Switch to **byte length** (`hk.byteLength`/`sk.byteLength`); drop the
`*2`. `data` already byte-measured. (The inline `row.hash_key.length * 2 + 16` estimator in
`do-partition.ts:575` is handled in M4.)

**Empty handling** (SPEC sharp-edge #5): any `sk === ""` / `?? ""` in the store becomes empty-`KeyBytes`
(`[]`, length 0). The schema default is `x''`.

Adapter: `do-partition.ts` still speaks `string` to the store until M4 — at the store-call sites in the
DO, wrap with `KeyCodec.encode(...)` / `encodeOptional(...)` on the way in and `KeyCodec.decode(...)` on
the way out. These temporary wrappers are removed in M4.

**Test:** update `partition-store.test.ts` to use `KeyBytes` fixtures; add astral + binary + empty cases;
assert `computeRangeSplitBoundaries` yields byte-monotonic boundaries whose child range scans tile the
parent (union = parent, no gaps/overlap) for the astral fixture that breaks today.

---

## M3 — Topology routing, boundaries, DO names

Files: `split-policy.ts`, `partition-context.ts`, `partition-id.ts`, `partial-range-topology.ts`.

**`partition-context.ts` (lines 55-59):** `rangePartition.{hashKey, startBoundary, endBoundary}` become
`KeyBytes` / `KeyBytes | null` in memory. **Do not** add a separate serialized field — see the
partition-id note below; the in-memory `KeyBytes` are derived from the opaque `partitionId` on load.

**`partition-id.ts` — this is mostly already byte-correct; tighten it:**
- `fromRangePartition` (lines ~175-202) already `TextEncoder`s the boundaries into a length-prefixed
  wire format. Change it to accept `KeyBytes` and store them **raw** (they're already canonical bytes —
  no `TextEncoder`). `decode` (lines ~146-172) returns `startBoundary/endBoundary/hashKey` as **`KeyBytes`**
  (raw subarrays via `asKeyBytes`), not `TextDecoder` strings. Net: the opaque `partitionId` *is* the
  serialized `KeyBytes` carrier — this resolves SPEC sharp-edge #2 (context serialization) with no new
  format, since `_partitionIdBytes` already survives structured clone / KV / RPC. Where `rangePartition`
  is needed in memory, decode it from `partitionId` rather than persisting it separately.
- Replace `encodeRangeComponent` (line 24, the buggy `charCodeAt`-multi-hex, SPEC *Related latent issue*)
  with a **byte-correct percent-encoder** (SPEC sharp-edge #3): per byte, pass through the safe-set
  (printable ASCII `0x21–0x7E` minus `% . space " \`) literally, else `%XX` (exactly two hex digits).
  Add a matching decoder. Used only for the human-readable `rangePartitionDoName` (lines ~30-35); it is
  **identity/serialization only and is never sorted or range-compared** (SPEC invariant — `PartitionId`
  carries identity in business logic). The DO name now encodes `KeyBytes` (including the `0xFF` tag) so
  binary boundaries are representable and reversible.

**`split-policy.ts`:**
- `shouldAllow` (line ~375): `inRange = compare(sk,start) >= 0 && (end===null || compare(sk,end) < 0)`
  via `KeyCodec.compare`. `start = rp.startBoundary ?? []`.
- `pickChildPartition` (lines ~460-487): the "largest `startBoundary <= sk`" pick uses `KeyCodec.compare`.
- `prepareSplit` (lines ~415-449): `starts`/`ends` arrays are `(KeyBytes | null)[]`; boundaries from M2's
  `computeRangeSplitBoundaries` flow straight through.

**`partial-range-topology.ts` (`learnPromotedKey`, line ~31)** and the bloom filter: hash the **encoded
`KeyBytes`** consistently with routing (SPEC sharp-edge #7) — reuse the M1 byte-hash primitive, not a
string hash.

**Test:** split-policy unit tests with astral boundaries (route/own decisions now match byte order); a
`partition-id` round-trip test (`fromRangePartition`→`decode` returns identical `KeyBytes`); a DO-name
percent-encode/decode round-trip incl. `.`, control bytes, high bytes, and the `0xFF` tag.

---

## M4 — DO partition + 2PC transaction path

Files: `do-partition.ts`, `partition/transaction-participant.ts`, `do-transaction-coordinator.ts`,
`transaction-limits.ts`, `partition/migration.ts`.

**`do-partition.ts`:**
- RPC entry points that receive keys (`putItem`/`deleteItem`/`getItem`/`getItemDirect`/`prepare`/`commit`/
  `readForTransaction`, lines ~221-710): the incoming key is already-encoded over RPC — `asKeyBytes` it
  (SPEC sharp-edge #4) and pass `KeyBytes` inward. Remove the M2 temporary store-call adapters.
- `inChildRange` (lines ~490-497): `sk >= lower && (upper===null || sk < upper)` → `KeyCodec.compare`.
  `lower = childCtx.rangePartition.startBoundary ?? []`.
- `getPartitionTransactionMetadata` (lines ~456-543): the range filter uses `inChildRange` (now bytes);
  the hash-child filter `row.sk === "" ? undefined : row.sk` (lines ~422/531) becomes an empty-`KeyBytes`
  check (`sk.length === 0 ? undefined : sk`).
- All `?? ""` sortKey defaulting (lines ~230/289/1100) → `encodeOptional` / empty `KeyBytes`.
- Cursors (`MigrationCursor`/`PendingTransactionCursor`) already `KeyBytes` from M2; the opaque cursor
  token serialization must carry bytes (SPEC sharp-edge #9) — if a cursor crosses RPC as a structured
  object the `Uint8Array` survives; if it's base64/string-encoded, encode the bytes explicitly.
- The inline promoted-key estimator `row.hash_key.length * 2 + 16` (line ~575) → `byteLength`.

**The `\0` key-joiner (byte-safety — a real cutover the SPEC only gestured at).** Three sites build
`` `${hashKey}\0${sortKey}` `` as a composite map/set key for 2PC keyset comparison:
`transaction-limits.ts:57`, `transaction-participant.ts:143-144`, `do-transaction-coordinator.ts:541/543`.
NUL is rejected in *string* keys, but **binary keys may legally contain `0x00`**, so the joiner is no
longer collision-proof. Replace the composite-key scheme with one of:
- a **length-prefixed byte join** of the two `KeyBytes` (e.g. `u32(hkLen) ‖ hk ‖ sk`), hashed/stringified
  to a stable map key (hex or latin1), or
- compare structurally on `(hk, sk)` `KeyBytes` pairs.

Pick the length-prefixed join for minimal churn; it is unambiguous for arbitrary bytes. Update the
comment block at `transaction-limits.ts:23-29` accordingly.

**`migration.ts`:** cursors and `insertItemIfAbsent`/`insertPromotedKey` calls now pass `KeyBytes`; no
logic change beyond types, since it threads the store's cursor/row types (M2).

**Test:** `transaction-participant.test.ts` and `migration.test.ts` with byte keys incl. a **binary key
containing `0x00`** to prove the new joiner is collision-proof; a range-child migration test over an
astral fixture proving locks land in the correct child (SPEC Bug 4).

---

## M5 — Public API widening + encode-at-entry / decode-at-exit

Files: `db.ts`, `types.ts`, `transaction-types.ts`, `transaction-limits.ts`, plus the `queryItems`
result path.

**Widen key fields to `string | Uint8Array`** (SPEC open-question #3) in `types.ts`
(`PutItemOptions`/`DeleteItemOptions`/`GetItemOptions`/`ItemKey`/`GetItemResult.item`/`PutItemResult`/
`DeleteItemResult`) and `transaction-types.ts` (`TransactionItem`, `RejectionReason`,
`ReadForTransaction*`, `TCWriteOperation`, `TCReadItem`, `InitiateWriteResponse` items). Result key
fields widen too (they come back via `decode`).

**Encode at entry** (`db.ts`, lines ~47-113): in `putItem`/`getItem`/`deleteItem`/`transactWriteItems`/
`transactGetItems`, after `validateItemKeys`, encode `hashKey`/`sortKey` to `KeyBytes` **once** and pass
encoded keys to `pickPartition` and the DO stub. `pickPartition`'s signature accepts the encoded key
(remove the M1 router-boundary adapter). Encode-once-reuse (SPEC *Performance*): one `TextEncoder` alloc
per key per request.

**`validateItemKeys`** (`transaction-limits.ts:30`): extend to (a) reject empty `hashKey` and empty
`sortKey` (SPEC empty-key decision), (b) reject lone-surrogate strings, (c) accept `Uint8Array` (the NUL
check only applies to strings; binary keys may contain any byte — the byte-safe joiner from M4 is what
makes that safe). Keep it the single key-validation boundary.

**Decode at exit:** `queryItems` (and transaction read results) return keys via `KeyCodec.decode` so the
caller gets `string` for UTF-8 keys and `Uint8Array` for binary keys. `getItem` echoes the caller's
input key (no decode). Confirm `decode(encode(k)) === k` holds end-to-end (M0 guarantees it for
well-formed input).

**Test:** end-to-end test that `put`/`get`/`delete`/`query` round-trip a **binary** `Uint8Array` key and
a UTF-8 string key through a real DO, and that a binary key and the same-bytes string key do **not**
alias (the `0xFF` tag).

---

## M6 — Hardening

- **Log readability** (SPEC sharp-edge #8): add a debug formatter (`hex`, or decoded string when valid
  UTF-8) and route key logging in `do-partition.ts` / `status()` / error paths through it. Raw
  `Uint8Array` must never print as a bare array in operational logs.
- **Lint/grep backstop** (SPEC *Enforcement*): a rule flagging `charCodeAt`/`codePointAt`/`localeCompare`/
  relational operators applied to key-shaped values **outside `key-codec.ts`**. Add to CI. Also flag new
  `` `${...}\0${...}` `` key-joiners.
- **Key size limits** (SPEC sharp-edge #3): enforce DynamoDB-style byte limits in `validateItemKeys`
  (e.g. 2KB hashKey, 1KB sortKey) measured on `KeyBytes`.
- **Full test matrix** consolidation: BMP, astral, binary, empty-key, and the `U+FFFF`-vs-emoji ordering
  case — exercised at unit (codec), store (BLOB ordering), topology (routing/splits), and end-to-end
  (public API) levels. Keep the M0 SQLite `ORDER BY` cross-check as a permanent regression guard.

---

## Things to confirm during implementation (cheap, do early)

1. **`js-xxhash` accepts `Uint8Array` + seed** (M0/M1). If not, byte-hash shim — never stringify bytes.
2. **workerd DO SQL binds `Uint8Array` as BLOB and returns BLOB as `ArrayBuffer`** under `STRICT` +
   `WITHOUT ROWID` (M2). Verify with the M0 cross-check test (it round-trips real BLOBs anyway).
3. **`partitionId` decode→`KeyBytes` is sufficient to reconstruct `rangePartition`** so no separate
   boundary serialization is needed (M3). If any path needs `rangePartition` before `partitionId` is
   parsed, cache the decoded `KeyBytes` alongside `_partitionIdBytes`.

## Suggested PR slicing

M0 and M1 are independent and small — land them first. M2 is the big one (schema + all store I/O);
it can land behind the M2 adapters with the rest of the app still string-typed. M3+M4 are tightly
coupled (topology boundaries ↔ DO ownership/2PC) but each keeps the build green; land M3 then M4. M5
flips the public surface and removes the last adapters. M6 is pure hardening and can trail.
