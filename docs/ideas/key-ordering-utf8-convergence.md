# Canonical Key Ordering: Converging JS and SQLite on UTF-8 Byte Order

Status: **idea / problem statement + design options**. No code changes yet.

## Context and Goal

fokosdb partitions a promoted hashKey's sort-key space into **range partitions** split at **boundary
strings**. Those boundaries — and sort keys in general — are compared by **two different engines that
do not agree on string order**:

- **JS engine — UTF-16 code-unit order.** Routing, ownership guards, and boundary *synthesis* all use
  JS string operators (`<`, `<=`, `charCodeAt`).
- **SQLite engine — UTF-8 byte order.** Keys are stored as `TEXT`; the default `BINARY` collation
  compares the UTF-8 bytes (the DO database is UTF-8). Every data scan (`sk >= ? AND sk < ?`,
  `ORDER BY sk`) runs in this order.

UTF-8 byte order equals Unicode code-point order. UTF-16 code-unit order equals code-point order **for
the entire BMP** but **diverges for supplementary-plane (astral) characters**, because UTF-16 encodes
them as surrogate pairs whose lead unit (`0xD800–0xDBFF`) sorts *below* BMP characters in
`0xE000–0xFFFF`.

The result: the keyspace partition that **JS routing** computes and the one that **SQLite data
placement** computes are not the same map. Boundaries are *born* in UTF-16 (synthesis) and *applied* in
SQL (scans). For sort keys that trip the divergence this causes **silent data loss, duplication,
misrouting, and lost transaction locks** in the range-split / promotion machinery.

**Goal:** make the whole system agree on one canonical key ordering — **UTF-8 byte order** — so JS-side
decisions and SQLite-side scans partition the keyspace identically for *all* inputs.

> Relationship to `queryItems`: the `queryItems` plan
> (`docs/agent-plans/query-items-design.md`, Appendix A) selected a **code-point `successor`** for
> `beginsWith` so it does not *add* a new instance of this bug. It does **not** fix the pre-existing
> sites below. This doc is the prerequisite/companion SPEC that fixes them system-wide.

---

## The Divergence, Precisely

Two *real* string values (not synthesized) can compare differently in the two engines:

```
a = "￿"             (U+FFFF)    UTF-8: EF BF BF
b = "😀" = "😀" (U+1F600)   UTF-8: F0 9F 98 80

JS  (UTF-16 code units):  0xD83D < 0xFFFF  ⇒  b < a  ⇒  (a < b) is FALSE
SQLite (UTF-8 bytes):     EF      < F0      ⇒  a < b  ⇒  (a < b) is TRUE
```

So `a < b` is **false in JS but true in SQLite**.

**Exact trigger.** A disagreement requires, at the first differing position, an **astral char**
(U+10000+, i.e. a surrogate unit `0xD800–0xDBFF`) on one side and a **BMP char in `0xE000–0xFFFF`** on
the other. Implications:

- Pure-BMP keys (ASCII, Latin, BMP CJK, etc.) are **100% safe** — the two orders agree across the whole
  BMP. At least one supplementary-plane character must be present to trigger anything.
- The `0xE000–0xFFFF` band is not exotic: it contains the **replacement character `U+FFFD`** (emitted
  on any invalid-input replacement), **private-use `U+E000–F8FF`**, and **Arabic presentation forms
  `U+FB50–U+FEFF`**. Emoji mixed with Arabic text, or with `�`, is enough.
- Astral-vs-astral and astral-vs-(BMP `< 0xE000`) **agree** in both engines; only astral-vs-`[E000,FFFF]`
  disagrees.

**Scope.** Confined to **range / promoted keys**. Hash-partition routing is `xxHash32(hashKey)` and is
immune; single-partition `getItem`/`putItem` never compare sort keys in JS. The bug lives entirely in
the sort-key boundary machinery.

---

## Concrete Bugs (pre-existing, silent)

### Bug 1 — Boundary synthesis produces UTF-8-inconsistent splits → data loss/duplication

`shortestSeparator` (`partition-store.ts:64-74`) and `computeRangeSplitBoundaries`
(`partition-store.ts:395-437`).

`computeRangeSplitBoundaries` orders and slices the items in **SQL** (`ORDER BY sk`, UTF-8) to pick
quantile keys, then computes each split boundary via `shortestSeparator(predecessor, boundary)` in
**UTF-16** (`charCodeAt`). With the example above (`predecessor = "￿"`, `boundary = "😀"`),
`shortestSeparator` compares code unit 0 (`0xFFFF` vs `0xD83D`), finds them different, and returns
`hi.substring(0,1)` = **`"\uD83D"` — a lone high surrogate**, a value whose UTF-8 sort position is
unrelated to where UTF-16 placed it.

The strict-monotonic guard (`:431-434`: `boundaries[i] <= lower`, `boundaries[i] <= boundaries[i-1]`)
validates monotonicity **in UTF-16**. A boundary list that is strictly increasing in UTF-16 can be
**non-monotonic in UTF-8**, so a child's migration scan `sk >= b_i AND sk < b_{i+1}` (SQL) becomes
inverted/empty.

**Failure:** during a range split, children migrate their slice via `queryRangeItemsPage`
(`sk >= start AND sk < end`, SQL). If the boundaries' UTF-8 positions differ from their intended
UTF-16 positions, the union of children's slices ≠ the parent's keyspace: some keys fall into **no**
child (silently dropped once the parent GCs), some into **two** (duplicated), some into a **different**
child than reads will later target.

### Bug 2 — Read/write routing targets the wrong child

`pickChildPartition` (`split-policy.ts:473-481`).

Routes a sort key to the child with the largest `startBoundary <= sk` using **JS** `<=`. But that
child's data was populated by the **SQL** `sk < end` migration scan. For an astral `sk` near a
boundary, JS routes the read/write to child A while the row physically migrated to child B.

**Failure:** `getItem` returns *not-found* for a row that exists in a sibling; `putItem` writes a
logical-duplicate of an existing key into the wrong child.

### Bug 3 — Ownership guard disagrees with stored data

`shouldAllow` (`split-policy.ts:375`): `inRange = sk >= start && (end === null || sk < end)` in **JS**.

The router and this guard are both JS, so they agree *with each other* — but the guard's notion of
"the range I own" disagrees with the **SQL-migrated data** actually present on the DO.

**Failure:** a key the guard accepts may be absent from the DO's data (appears empty); a key present in
the DO's data may be guard-`reject`ed on read, surfacing as a spurious "out of owned range" routing
error.

### Bug 4 — Transaction-lock migration misassigns/drops locks (correctness)

`inChildRange` (`do-partition.ts:492-497`).

During a range split, `getPartitionTransactionMetadata` pages `pending_transactions` from **SQL** and
filters which locks belong to each range child with `sk >= lower && sk < upper` in **JS**. An
astral-keyed lock can be assigned to the wrong child or dropped.

**Failure:** an in-flight 2PC lock is lost or misplaced → the transaction can commit incorrectly or
leave a key stuck locked. This is worse than a stale read — it is a correctness violation in the
transaction path.

### Related latent issue (not ordering) — `encodeRangeComponent`

`partition-id.ts:24`: `charCodeAt(0).toString(16).padStart(2,"0")`. For any code unit > `0xFF` (all
CJK, surrogate halves, …) this emits **3–4 hex digits** (`"中"` → `"%4E2D"`), which is not valid `%XX`
percent-encoding. It remains *injective* (DO-name uniqueness holds, so not active corruption), but it
is mislabeled as percent-encoding and would break any byte-wise decode. Worth fixing in the same effort
since boundaries will become byte sequences.

---

## What "Correct" Means

DynamoDB avoids all of this by never using UTF-16: String keys are ordered by **UTF-8 byte
comparison**, Binary keys by **unsigned byte comparison**, and every operator (`begins_with`,
`BETWEEN`, comparisons) evaluates in that *same* byte space. Compare-space == prefix-space ==
storage-space ⇒ inherently consistent.

Key realization for us: **SQLite already orders the DynamoDB way.** `TEXT` + `BINARY` = UTF-8 byte
order. The stored data is already correct; the bug is entirely the **JS side using UTF-16**. The fix is
to drag every JS-side key comparison and synthesis onto UTF-8 byte order.

---

## Constraints (clean slate)

**Not yet deployed anywhere.** There is no production data, so there is **no data migration, no table
rebuild, no DO-name backward-compatibility, and no big-DO stall** to manage. We define the schema and
key encoding as BLOB-native from the start — the existing migrations (`SQLSchemaMigrations`, ids 1–4)
can simply be changed in place to use `BLOB` key columns. This removes the entire migration/rollout
problem space; what remains is the forward design.

## Chosen Direction (decided)

**A `KeyCodec` abstraction (the "Option C" scaffolding) whose internal representation is byte/BLOB keys
("Option A").** All key comparison and synthesis is centralized behind one module; underneath, keys are
canonical byte sequences stored as `BLOB` and compared by `memcmp`. Because the codec absorbs
string-vs-bytes uniformly, **DynamoDB-style binary (`Uint8Array`) keys are exposed at the client API**
at near-zero marginal cost.

> Alternatives considered and rejected: keeping `TEXT` with code-point arithmetic ("Option B") fixes the
> bugs with no migration but can never represent byte-exact boundaries or binary keys; doing Option A
> *without* the codec leaves the "did we catch every raw `<`?" risk. The codec + byte representation gives
> both correctness and binary keys behind a single enforceable boundary.

### The `KeyCodec` boundary

A single module owns everything about key bytes and ordering; **no other code compares or synthesizes
keys directly**. Sketch of responsibilities:

- `encode(key: string | Uint8Array): Uint8Array` — public key → canonical, **type-tagged** bytes.
- `decode(bytes: Uint8Array): string | Uint8Array` — canonical bytes → original-typed key (round-trip).
- `compare(a: Uint8Array, b: Uint8Array): number` — unsigned byte compare (matches `BLOB` `memcmp`).
- `successor(prefix: Uint8Array): Uint8Array | null` — prefix upper bound in byte space (`null` =
  unbounded; the all-`0xFF` case).
- `shortestSeparator(lo, hi): Uint8Array` — byte-space separator (replaces the UTF-16 version).

Every current offender routes through it: `shouldAllow`, `pickChildPartition`, `inChildRange`,
`computeRangeSplitBoundaries`, the store scans, and `encodeRangeComponent`.

### Type tagging (round-trip + collision avoidance)

The encoded bytes must carry a discriminator so reads can return the right type and so a string and a
binary key cannot alias (e.g. `"A"` vs `[0x41]`).

**Decided: `0xFF`-lead for binary only.** Strings encode to **raw UTF-8** (which never contains a
`0xFF` byte, so a string can never start with `0xFF`); binary encodes to `0xFF || rawBytes`. On read,
first byte `0xFF` ⇒ binary, else decode as UTF-8. Rationale:

- **Empty/absent sort key stays the empty byte sequence `[]`** — the global minimum — preserving the
  current `sk DEFAULT ''` semantics (every single-key item has `sk=''`). An explicit-tag scheme would
  make the empty string `[0x00]` and shift that.
- **Zero overhead for the common (string) case** — only binary keys pay the 1-byte lead.
- `0xFF` is a guaranteed-safe discriminator (invalid as any UTF-8 byte), so no escaping is needed.

**Why `[]` and not SQL `NULL` (decided).** `NULL` looks tempting (it sorts first, no `DEFAULT`
needed) but breaks the design in exactly the ways this effort exists to prevent:

- **NULL vanishes from range scans.** Every SQL comparison with `NULL` is `NULL` (unknown ⇒ falsy), so
  a `NULL`-keyed row matches *neither* `sk >= ?` nor `sk < ?` — it silently drops out of every range /
  migration scan. That is the precise "row falls into no child" failure mode we are eliminating.
  Avoiding it would require `(sk IS NULL OR sk >= ?)` smeared across every scan — the opposite of "one
  comparison rule everywhere."
- **NULL breaks single-key uniqueness.** The PK is `(hk, sk)` (`WITHOUT ROWID, STRICT`). In index
  semantics NULLs are distinct, so two `putItem`s with no sort key would become `(hk, NULL) ≠ (hk,
  NULL)` → **duplicate rows for one logical item** instead of an upsert. (`NOT NULL` + `WITHOUT ROWID`
  also reject NULL PK columns outright, so we'd be fighting the engine for a worse result.)
- **NULL reintroduces per-comparison branching** in the codec/JS (`successor`, `shortestSeparator`,
  binds), defeating the total-order-under-one-`memcmp` model. `[]` is a normal inhabitant of that
  order; `null` is a second kind requiring special-casing everywhere.

**Empty keys are rejected; `[]` comes only from an absent key (decided).** A sort key is either
*absent* (caller omits `sortKey?: string`) or a non-empty value. Explicit empty
(`sortKey: ""` / `new Uint8Array(0)`) and empty `hashKey` are **rejected at `validateItemKeys`**
(the existing key-validation boundary that already forbids NUL), matching DynamoDB's "key attributes
cannot be empty." Consequences: the API maps `undefined → []` directly, **`encode()` never returns
`[]`**, `[]` has exactly one meaning, and the degenerate empty-binary key `[0xFF]` cannot exist.

**Round-trip is exact except for malformed UTF-16 (decided: reject).** For every well-formed input,
`decode(encode(k)) === k` — strings round-trip through UTF-8, binary round-trips through the `0xFF`
lead — so the value `getItem` echoes (the caller's input) and the value `queryItems` decodes from
storage are identical. The **only** divergence is a string containing a **lone/unpaired surrogate**
(e.g. `"\uD83D"`), which `TextEncoder` would silently rewrite to `U+FFFD`. Such a string is invalid
UTF-16, not a real key, so we **reject it at `encode`** rather than accept the lossy normalization;
this also rules out the old code's habit of synthesizing lone surrogates.

Within a type, order is the natural byte order (UTF-8 for strings = DynamoDB String semantics; raw
bytes for binary = DynamoDB Binary semantics); all string keys sort before all binary keys (the `0xFF`
lead). fokosdb is schemaless on keys, so mixing the two is allowed and well-defined.

### Why comparisons stay simple (the tag does not ripple)

Once a key is encoded, **every** comparison anywhere — ordering, range bounds, equality, sort, SQLite
`sk < ?` — is a single unsigned byte compare of the encoded forms. The tag is just leading bytes that
participate naturally; there is no per-comparison type branching:

```
"A"     → [0x41]            (string: raw UTF-8)
[0x41]  → [0xFF, 0x41]      (binary: 0xFF lead)

compare("A", [0x41]) = memcmp([0x41], [0xFF, 0x41]) → 0x41 < 0xFF → string < binary
```

`KeyCodec.compare` is one `memcmp`; SQLite's `BLOB`/`memcmp` yields the identical result on the stored
bytes. The synthesis paths are pure byte arithmetic too and never special-case the tag:
`successor([0xFF,0x01]) → [0xFF,0x02]`; `shortestSeparator` just finds the first differing byte. So all
type-awareness lives in exactly two boundary functions — `encode` (prepend `0xFF` for binary) and
`decode` (peek the first byte) — and **everything between them sees opaque, uniformly-comparable
bytes**. Net result: one comparison rule everywhere, replacing today's mix of UTF-16 `<`, `charCodeAt`,
and SQL comparison.

The single invariant to uphold: **never compare a raw user value against an encoded key** — encode at
entry, compare/route/store as bytes, decode at exit. See *Enforcement* below for how this is made a
compile-time guarantee rather than a discipline.

### What exposing `Uint8Array` keys touches (the "nearly free" caveats)

The engine is type-agnostic, but three edges change:

1. **Result types widen** to `string | Uint8Array` (`getItem`, `queryItems`, transaction reads) — the
   codec's `decode` picks the type via the tag.
2. **Collision/ordering** handled by the type tag above.
3. **Edge encoders** — `encodeRangeComponent` (DO names) must encode arbitrary bytes; key size limits
   move to bytes. These need rework for byte boundaries regardless of binary keys.

### Enforcement: a nominal key-bytes type

The "encode at entry, decode at exit, only ever compare bytes" invariant should be a **compile-time
guarantee**, not a code-review discipline. The mechanism is a *nominal* (branded) type for encoded keys
that is structurally a `Uint8Array` but distinct in the type system, so raw strings/arrays and encoded
keys cannot be mixed up:

```ts
// A branded byte string. Structurally a Uint8Array, but you cannot pass a plain
// Uint8Array/string where a KeyBytes is expected, nor vice versa.
declare const KEY_BRAND: unique symbol;
export type KeyBytes = Uint8Array & { readonly [KEY_BRAND]: true };

export const KeyCodec = {
  encode(key: string | Uint8Array): KeyBytes { /* 0xFF-tag binary; raw UTF-8 string */ },
  decode(k: KeyBytes): string | Uint8Array { /* peek tag */ },
  compare(a: KeyBytes, b: KeyBytes): number { /* memcmp */ },
  successor(prefix: KeyBytes): KeyBytes | null,
  shortestSeparator(lo: KeyBytes, hi: KeyBytes): KeyBytes,
};
```

Why this enforces correctness:

- **The only way to obtain a `KeyBytes` is `KeyCodec.encode`.** No other code can fabricate one, so a
  value typed `KeyBytes` is *guaranteed* already-encoded. A function that takes `KeyBytes` can trust its
  input without re-checking.
- **A raw `string`/`Uint8Array` is rejected** wherever a `KeyBytes` is required — so "forgot to encode"
  is a **type error**, not a silent runtime bug. This is exactly the failure mode that produced bugs
  1–4 (raw key compared in JS).
- **Ordering helpers only accept `KeyBytes`.** `compare`/`successor`/`shortestSeparator` take `KeyBytes`,
  so the only comparison available on keys is the byte-correct one. There is no `<` on `KeyBytes` that
  does the wrong thing — callers must go through `KeyCodec.compare`.
- **Store/routing signatures speak `KeyBytes`.** `shouldAllow`, `pickChildPartition`, `inChildRange`,
  the store scans, and the SQL bind params all take/emit `KeyBytes`. Boundaries persisted in partition
  context become `KeyBytes` (serialized as bytes). The public API (`hashKey`/`sortKey`/`prefix`) stays
  `string | Uint8Array` and is encoded at the FokosDB/DO entry points.

Backstops beyond the type (defense in depth, since branding is erased at runtime and `Uint8Array` still
*has* no `<` but `string` keys could still be compared if a signature is wrong): a lint/grep rule
flagging `charCodeAt`/`codePointAt`/relational operators on anything key-shaped outside `KeyCodec`, and
a unit test asserting `compare` matches SQLite's `ORDER BY` on the astral fixtures.

---

## Performance & Allocations

A natural worry: encoding every string key to UTF-8 bytes sounds like it clones keys constantly. It does
not — the cost is bounded and largely already paid today.

- **Encode once at the entry boundary, carry `KeyBytes` through.** Routing, split-forwarding, ownership
  checks, and the store all operate on the same encoded value. We do **not** re-encode per comparison;
  it is ~one `TextEncoder` allocation per key per request (hashKey, plus sortKey if present).
- **The SQLite round-trip is ~cost-neutral vs today.** The DO database is UTF-8, so binding a JS string
  as `TEXT` *already* converts UTF-16→UTF-8 inside the V8↔SQLite binding (and decodes on read). Moving to
  explicit `encode`/`decode` + `BLOB` makes that same conversion explicit and reusable rather than hidden.
  Storage size is unchanged (UTF-8 bytes either way; binary keys pay +1 byte for the `0xFF` lead).
  `getItem` still echoes the caller's input key (no decode); only `queryItems` decodes keys it scanned.
- **The only genuinely-new cost** is that JS-side comparisons (routing/splits), which today compare the
  in-memory string with native `<` for free, now need the encoded bytes. With encode-once-reuse that is
  one small allocation per request — negligible beside a Workers RPC hop or a SQLite query — and each
  downstream comparison becomes a `memcmp`, which is *cheaper* per-comparison than a correct
  code-point/string comparator. Across the several comparisons a request makes, this can net out
  favorably.
- **This is a property of byte ordering, not of `Uint8Array` support.** A string-only design that fixed
  the UTF-16 bug the same way would clone identically; binary keys add only the `0xFF` lead. The sole way
  to avoid the encode-for-comparison clone was Option B (compare strings in code-point order in place),
  which instead moves the cost to per-comparison CPU + branching and cannot represent binary keys or
  byte-exact boundaries.

## Sharp Edges / What Breaks (pre-implementation inventory)

Moving keys from `TEXT` strings to canonical `KeyBytes` ripples beyond the comparison sites. These are
the non-obvious things to handle (call-site inventory for the implementation plan):

1. **Hash routing seed — `hashChildIndex(hashKey + (depth + 1), …)`** (`hash-primitives.ts`).
   **Decided: depth-seeded hashing.** The concat is incidental; its only purpose is per-level
   decorrelation. Instead of salting the *input*, vary the *seed*: `xxHash32(keyBytes, seedForDepth(d))`
   where `seedForDepth` mixes `GOLDEN_RATIO` with the depth (e.g. `GOLDEN_RATIO ^ (d * PRIME)`). This
   hashes the **same** key bytes at every level with **zero per-level allocation** (no concat/temp
   buffer), and different seeds give independent hash streams — the decorrelation we want, the idiomatic
   xxHash way. `hashRootIndex` and `hashChildIndex` both hash the **encoded** `KeyBytes` consistently.
   (Clean slate ⇒ changed hash values are fine; only internal consistency matters. Confirm our
   `xxHash32` accepts a `Uint8Array` input + seed arg, else a tiny shim.)
   - **Overflow:** not a correctness risk (depths are tiny — `u8` — and a seed only needs to be
     deterministic + distinct per depth, for which 32-bit modular arithmetic is ideal). Compute it in
     defined 32-bit space to dodge JS float/bitwise footguns:
     `seedForDepth(d) = (GOLDEN_RATIO ^ Math.imul(d, PRIME)) >>> 0`, `PRIME` **odd** (makes
     `d ↦ d·PRIME mod 2^32` a bijection ⇒ no per-depth collisions). `Math.imul` = true 32-bit multiply;
     `>>> 0` = unsigned seed. (`(GOLDEN_RATIO + d) >>> 0` likely suffices given xxHash avalanche.)
2. **Partition context carries boundaries as keys.** `PartitionContextResolved.rangePartition`'s
   `hashKey`/`startBoundary`/`endBoundary` are strings today and are **serialized to DO KV storage,
   passed over RPC, embedded in DO names, and encoded into the opaque `partitionId`**. If they become raw
   `Uint8Array`, `JSON.stringify` mangles them. **Decided:** keep them in the persisted/serialized
   context as the **readable percent-encoded string form** (see #3), decoded to `KeyBytes` in memory —
   one form covers KV, RPC, DO names, and logs. Touches `partition-context.ts`, `partition-id.ts`, KV
   load/store, and `status()`.
3. **DO-name / serialized encoding — byte-correct percent-encoding with a safe-set (decided).** Replace
   the buggy `encodeRangeComponent` (`charCodeAt`, multi-hex) with a true byte-oriented percent-encoding:
   each byte either passes through literally or becomes `%XX` (exactly two hex digits). The **safe-set is
   printable ASCII `0x21–0x7E` minus reserved chars** — `%` (the escape), `.` (our DO-name component
   delimiter), and `space`/`"`/`\` (JSON/log cleanliness). Rationale (not about `idFromName`, which
   accepts any string of any length — it hashes internally): (a) our own composite name format splits on
   `.`, so a literal `.` in a key would collide with the delimiter; (b) ASCII `0x00–0x1F`/`0x7F` are
   control chars — passing them through garbles logs and enables **terminal-escape / log injection**. So
   `user:123/profile` stays readable (`:`/`/` pass through), while delimiters, control, high, and binary
   bytes (`0xFF` tag) escape to `%XX`. Reversible to exact `KeyBytes`; readable for the common ASCII
   case; compact (~1 char/byte vs hex's 2). Not order-preserving — fine, ordering is always on the
   in-memory `KeyBytes`. Define explicit **key size limits** (e.g. DynamoDB-style 2KB hashKey / 1KB
   sortKey) for sanity even though `idFromName` itself is length-agnostic. Optional later nicety:
   UTF-8-aware pass-through so non-ASCII text (`café`) stays literal too. **Invariant (confirmed):** this
   percent-encoded form is identity/serialization only — **it is never sorted or range-compared.** All
   ordering happens on the in-memory `KeyBytes`; `PartitionId` carries identity in business logic, so the
   DO-name form being non-order-preserving is harmless by construction. Worth stating as an enforced rule
   so it stays true once boundaries are bytes.
4. **RPC erases the brand.** `KeyBytes` is compile-time only; over RPC it arrives as a plain
   `Uint8Array`. RPC entry points need a trusted re-brand (`asKeyBytes`) — and a clear rule that RPC
   payloads are already-encoded.
5. **Empty-key checks.** Patterns like `sk === ""` / `sk ?? ""` (e.g. the `row.sk === "" ? undefined`
   filters in `do-partition.ts`) become byte-length checks; schema default `DEFAULT ''` → `DEFAULT x''`;
   absent sortKey → empty `KeyBytes` `[]`.
6. **Size estimators drive split/promotion triggering.** `estimateRowBytes`/`estimateItemBytes` use
   `hk.length`/`sk.length` (UTF-16 units) and `*2` factors. With byte keys these must use byte lengths —
   this affects **when** partitions split/promote (correctness of thresholds), not just reported numbers.
7. **Bloom filter hashing.** `PartialRangeTopology.learnPromotedKey(hashKey)` hashes the hashKey; must
   hash the encoded bytes consistently with routing.
8. **Log readability.** Keys appear throughout `console.log/error` and `status()`. Raw bytes print as
   unreadable arrays — add a debug formatter (hex, or decoded string when valid UTF-8) so operational
   logs stay usable.
9. **Cursor types.** Migration/scan cursors (`MigrationCursor`, `PendingTransactionCursor`,
   `queryItems` cursor) carry `hk`/`sk` — become `KeyBytes`; their `>`/`=` comparators go through the
   codec, and the opaque cursor token must serialize bytes.
10. **Tests & fixtures.** Existing tests assume string keys, string DO-name fragments, and UTF-16
    behavior; they will need updating. Add the astral/binary/empty fixtures and a `compare` vs SQLite
    `ORDER BY` cross-check.

None of these are blockers — they are the concrete cutover surface. The two that most shape the design
(and deserve a decision before coding) are **#1 (routing seed)** and **#2 (context serialization of
byte boundaries)**.

## Open Questions for the Design Discussion

1. **Validation of ground truth — narrowed to a `BLOB` `memcmp` cross-check (decided).** Going
   `BLOB`-native makes `PRAGMA encoding` irrelevant: `BLOB` comparison is always unsigned `memcmp` of the
   exact stored bytes, **independent of the DB text encoding and of any collation** (collations apply only
   to `TEXT`). The encoding question was a `TEXT`-era artifact — with `TEXT`+`BINARY`, a UTF-16 database
   would compare UTF-16 bytes and not match our UTF-8 `KeyBytes`; with `BLOB` we control the bytes and
   SQLite compares precisely those. (workerd/DO SQLite uses the SQLite default UTF-8 anyway and exposes no
   way to change it, but we no longer depend on that.) So the only check worth running is the trivial,
   encoding-independent one: insert `BLOB` fixtures (incl. `U+FFFF` vs emoji, binary, empty) and assert
   `ORDER BY` matches `KeyCodec.compare`. `PRAGMA encoding` is optional curiosity, not a dependency.
2. **Enforcement** — resolved via the nominal `KeyBytes` type (see *Enforcement* above); remaining
   detail is the exact lint/grep backstop rule.
3. **Public API surface** — confirm `hashKey`/`sortKey`/`prefix` all accept `string | Uint8Array`
   consistently across `putItem`/`getItem`/`deleteItem`/`queryItems`/transactions, and that result types
   widen to `string | Uint8Array` via `decode`.
4. **`est_row_bytes` / size estimates** — the estimators (`estimateRowBytes`, `estimateItemBytes`) use
   `hk.length`/`sk.length`; revisit for byte-length semantics once keys are bytes.
5. **`computeRangeSplitBoundaries`** — currently returns `string[]`; becomes `Uint8Array[]` boundaries
   via the codec's byte-space `shortestSeparator`.

---

## Next Step

Expand into a full implementation plan: the `KeyCodec` interface, the BLOB schema (migrations 1–4
edited in place), the exhaustive call-site cutover list (`shouldAllow`, `pickChildPartition`,
`inChildRange`, `computeRangeSplitBoundaries`, `shortestSeparator`, the store scans,
`encodeRangeComponent`, the size estimators), the public-API widening to `string | Uint8Array`, and the
test matrix (BMP, astral, binary, empty-key, and the `U+FFFF`-vs-emoji ordering case).
