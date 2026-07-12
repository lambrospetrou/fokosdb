# Item data kinds: bytes / text / JSONB

Status: proposed
Scope: `items` (and `pending_transactions`) data storage, the `db.ts` client boundary, the
partition store read/write paths, size estimation, and DO→DO migration. Filtering/query features
that consume JSONB are **out of scope** here but this design is the foundation they build on.

Not-in-production note: there is **no backwards-compatibility requirement**. We can change the
`CREATE TABLE` statements in place (and reset dev state) rather than writing rebuild migrations.

---

## 1. Goal

Today `items.data` is a single `ANY` column holding either a `string` or a `Uint8Array`, and we
tell the two apart purely by SQLite's storage class (TEXT vs BLOB) — `fromSqlData` reads it back
with a `typeof value === "string"` check. There is no discriminator column.

We want three logical kinds:

- **bytes** — a `Uint8Array`, stored verbatim as a BLOB. Opaque.
- **text** — a `string`, stored as TEXT. Opaque.
- **json** — a JSON object/array, stored as **JSONB** so we can later run `jsonb_extract` / `->>` /
  `json_each` filters and per-attribute expression/partial indexes, and eventually server-side
  `jsonb_set` mutations — all only on json rows.

And we want a **super-fast, SQL-visible way to know which row is which**, so filtering only ever
touches json rows and never runs a JSON function over a bytes/text row.

The accepted public value type is:

```ts
data: string | Uint8Array | JsonValue
```

where for now `JsonValue` at the **top level is restricted to objects and arrays** (no bare
top-level primitives). See §3.

---

## 2. Decisions and reasoning

### 2.1 Three kinds; a bare string stays opaque TEXT (not JSON-wrapped)

We do **not** `JSON.stringify` a bare string into a JSON string scalar. Reasoning:

- The input **type is already an unambiguous signal**: a `string` means opaque text, a
  `Uint8Array` means bytes, an object/array means JSON. There is nothing to infer, so wrapping buys
  nothing.
- **A scalar has no structure to filter on.** JSONB's value is entirely in structural access
  (`jsonb_extract`, `json_each`, path indexes). A wrapped string exposes no paths. There is no
  `json_string_length`; to get a JSON string's length you must `length(data ->> '$')`, whereas a
  TEXT column answers `length(data)` directly and is directly indexable. Wrapping is pure overhead
  plus an extraction hop, with zero capability gain.
- **Round-trip fidelity + no double-encoding footgun.** A TEXT string returns byte-identical. If we
  wrapped, a user who legitimately stores JSON *text* (e.g. `'{"a":1}'`) would get it double-encoded
  to `"{\"a\":1}"` and it *still* wouldn't be path-filterable. The right way to get JSON semantics is
  to pass the object, which routes to the json kind.

### 2.2 Top-level primitives excluded initially

`JsonValue` permits nested primitives, but the **top-level** accepted type is object/array only. We
start restricted because it keeps the "object/array ⇒ json" runtime rule trivial and unambiguous
against `string`/`number`/`boolean`. Relaxing to accept top-level scalars later is additive
(see §9).

### 2.3 An explicit integer `data_kind` column — because JSONB collides with bytes

**SQLite JSONB is itself a BLOB.** `jsonb('{"a":1}')` returns SQLite's binary-JSON encoding, which
comes back to JS as an `ArrayBuffer` — indistinguishable by storage class or `typeof` from a user's
raw `Uint8Array`. So the moment JSONB exists, we have three logical kinds over two storage classes:

| kind  | stored as | storage class |
|-------|-----------|---------------|
| bytes | BLOB      | blob          |
| json  | JSONB     | blob ← collides |
| text  | TEXT      | text          |

The `typeof` trick can still peel off text, but it can no longer tell json from bytes. We therefore
add a single `data_kind INTEGER` column. Reasoning for a column over the alternatives:

- It is **SQL-visible**, so filters read `WHERE data_kind = <json> AND jsonb_extract(...)` and the
  read path can decide *before* selecting whether to decode with `json(data)`. This is required, not
  cosmetic — see §5/§6.
- It enables **partial indexes** keyed on `data_kind = <json>` (§8), so json filtering is fast and
  bytes/text rows add zero index bloat.
- The alternative — detecting JSONB at query time with `json_valid(data, 8)` — is both **slow**
  (an unindexable per-row function call) and **unsound** (arbitrary user bytes can coincidentally be
  valid JSONB). Prefixing a type tag inside the blob is also out: `jsonb_extract` needs the column to
  *be* the JSONB, unshifted.

Kind codes (stored as the integer; a string discriminant is used in TS types, see §3):

```
Bytes = 0, Text = 1, Json = 2
```

### 2.4 The boundary owns JS↔text; the DO owns text↔JSONB

Only the DO has SQLite, so `jsonb()`/`json()` (text↔binary JSON) **must** run inside the DO. And the
`db.ts` client (`FokosDB`) may run in a different isolate than the partition DO. So we split
responsibilities and, critically, keep the DO receiving **only `string | Uint8Array`**:

```
WRITE (put):
  user JsonValue --[db.ts]--> JSON.stringify --> {kind:Json, data:<text>} --RPC-->
    [store] data = jsonb(<text>) --> JSONB blob in items.data (data_kind=Json)

READ (get):
  items.data JSONB --[store]--> SELECT json(data) --> <text> --RPC-->
    {kind:Json, data:<text>} --[db.ts]--> JSON.parse --> JsonValue --> user

MIGRATION (DO→DO):
  source items.data JSONB blob --[store raw read]--> {kind:Json, data:<JSONB blob>} --RPC-->
    [target store] INSERT data = ?  (blob verbatim, no re-encode) (data_kind=Json)
```

Consequences and reasoning:

- **`JSON.stringify` / `JSON.parse` happen exactly once, at the `db.ts` boundary.** The DO never
  serializes or parses a JS object; it only does SQL-level `jsonb()`/`json()`.
- Because the DO only ever holds `string | Uint8Array`, all DO-level **transfer-size** math
  (`estimateItemBytes`, `estimatePendingTxBytes`) stays a trivial
  `typeof data === "string" ? data.length : data.byteLength`. Those estimators care about **rough
  transfer/batch size, not exact storage bytes**, so this is correct and needs no change (§7).

### 2.5 `est_row_bytes` becomes a STORED generated column (delete the JS estimator)

Storage-accurate row size is a separate concern from transfer size (§2.4). It feeds
`key_size_estimates` → promotion/split, so it must reflect **actual stored bytes**. The current
`estimateRowBytes` computes the data term in JS as `data.length`, which is UTF-16 code units (already
wrong for multi-byte text) and has **no relationship to the JSONB byte size**. Any JS-side formula
guesses at a representation SQLite owns and drifts whenever the encoding changes.

Fix: let SQLite measure it. `octet_length(X)` returns the true encoded byte count (UTF-8 bytes for
TEXT, blob bytes for BLOB/JSONB) uniformly across kinds. We make `est_row_bytes` a **STORED generated
column**:

```sql
est_row_bytes INTEGER GENERATED ALWAYS AS
  (octet_length(data) + octet_length(hk) + octet_length(sk) + K) STORED
```

- Exact for the variable part (data + keys); the small, stable integer-columns + record-header
  remainder stays folded into the constant `K`. There is no cheap per-row function for the full
  on-disk record size (`dbstat` is per-page, too coarse), so this is the sweet spot.
- **Drift-proof and self-maintaining at the row level** — SQLite recomputes on every insert/update,
  so no JS path can desync it.
- `computeRangeSplitBoundaries` (streams `est_row_bytes` per row) and `rebuildKeySizeEstimates`
  (`SUM(est_row_bytes)`) are unchanged — STORED means it's a real column.

Because there's no compat constraint, we bake the generated column straight into the `items`
`CREATE TABLE` (SQLite forbids adding a STORED generated column via `ALTER TABLE`, but a fresh
`CREATE TABLE` has no such restriction — so **no table-rebuild migration is needed**).

`estimateRowBytes` (and its tests) are **deleted**.

**Evolving the formula later** (e.g. tuning `K`): a STORED generated column's expression cannot be
`ALTER`ed in place (SQLite has no `ALTER COLUMN`), and it cannot be re-added via `ALTER TABLE ADD
COLUMN` (that path only allows VIRTUAL). Changing it is a **table-rebuild migration** —
`CREATE` new table with the new expression → `INSERT INTO new SELECT … FROM old` → `DROP old` →
`RENAME`. This is **data-preserving** (never a destructive drop) and actually the correct way to
re-derive existing rows, since STORED values are materialized and would not recompute on an
expression change anyway. We accept this cost because formula changes are rare and there's no compat
constraint. We deliberately reject the two cheaper-to-evolve alternatives:
- **VIRTUAL generated** (evolvable via `DROP COLUMN` + `ADD COLUMN`, recomputed on read) would force
  `computeRangeSplitBoundaries` and `rebuildKeySizeEstimates` to load every row's `data` blob to
  recompute `octet_length`. STORED lets the split scan read only the materialized integer and never
  touch the (potentially large) data — worth more than cheap formula edits.
- **A plain column set in the write SQL** (change the INSERT + one `UPDATE` backfill, no schema
  change) gives up the self-maintenance/no-drift property that motivated the generated column.

### 2.6 The `key_size_estimates` rollup delta stays explicit (sourced from SQLite)

The per-`hk` `key_size_estimates.est_bytes` SUM is not maintained by a generated column. We keep the
read-old / apply-new delta in `upsertItem`/`deleteItem`, but now it reads **exact** values from
SQLite (`RETURNING est_row_bytes` for the new value; the pre-write `SELECT` for the old). We
deliberately do **not** push this into triggers: triggers would inflate the `rowsRead`/`rowsWritten`
counts we assemble into RPC `meta` and add per-write cost.

### 2.7 Migration copies the stored representation verbatim

DO→DO migration reads items and re-inserts them into child DOs. For json rows it copies the **raw
JSONB blob verbatim** (both sides are DOs with SQLite; JSONB is portable) rather than
`json(data)` → text → `jsonb(text)`. Reasoning: avoids per-item parse/re-encode CPU on a bulk
operation and avoids any canonicalization drift. This is why the migration read path differs from the
public read path (raw vs `json()`-decoded), and why `insertItemIfAbsent` (migration-only) binds the
blob directly while `upsertItem` (fresh puts + commits) wraps text with `jsonb()`.

### 2.8 Pending-transaction data stays un-encoded text/bytes

`pending_transactions` is never queried by JSON path, so we do **not** JSONB-encode its data. We store
the raw `text`/`bytes` plus a `data_kind` tag. On commit, `#applyCommitItems` reads the pending
row and calls `upsertItem({ kind, data })`, so the single json-encode path (`jsonb(text)`) in
`upsertItem` is reused. This keeps `upsertItem` with exactly one json input form (JSON text), and
keeps the verbatim-blob path confined to migration (`insertItemIfAbsent`).

---

## 3. Types

```ts
// ---- public value type (types.ts) ----
export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };
// Top-level accepted composites only (start restricted; see §9).
export type JsonComposite = JsonValue[] | { [key: string]: JsonValue };

// PutItemOptions.data, transaction op data, etc.:
data: string | Uint8Array | JsonComposite;

// ---- kind discriminant ----
// The public/TS discriminant is a readable string literal (it is user-visible on read results, §6).
// The on-disk `data_kind` column stores the compact integer code = the array index.
//
// ONE source of truth: the array. Type is inferred from it; both lookups are just index math, so
// nothing can drift. Adding a kind = one array entry.
export const DATA_KINDS = ["bytes", "text", "json"] as const; // index = on-disk code
export type DataKind = (typeof DATA_KINDS)[number];           // "bytes" | "text" | "json"
// name → code: DATA_KINDS.indexOf(kind)   |   code → name: DATA_KINDS[code]
```

Three discriminated-union representations model the pipeline (the "kind property + data typed by
kind" the DO layer uses). Requests and responses are both `{ kind, data }` unions — symmetric:

```ts
// A) Encoded for the wire / store WRITE. JSON already stringified at the boundary.
//    The DO only ever sees string | Uint8Array here.
export type EncodedItemData =
  | { kind: "bytes"; data: Uint8Array }
  | { kind: "text";  data: string }
  | { kind: "json";  data: string }; // JSON text → store as jsonb(data)

// B) Raw stored form, read verbatim for DO→DO migration (no JSON decode).
export type StoredItemData =
  | { kind: "bytes"; data: Uint8Array }
  | { kind: "text";  data: string }
  | { kind: "json";  data: Uint8Array }; // raw JSONB blob, copied verbatim

// C) Decoded for public READ (json rebuilt at the db.ts boundary).
export type DecodedItemData =
  | { kind: "bytes"; data: Uint8Array }
  | { kind: "text";  data: string }
  | { kind: "json";  data: JsonValue };
```

Public read results (`GetItemResult`, `queryItems` items, `transactGetItems` /
`readForTransaction` items) expose **both** the reconstructed value and its `kind`
(`DecodedItemData`), mirroring how the write path is kind-aware. Callers can switch on
`item.kind` instead of `typeof`/`instanceof`/`Array.isArray`. See §6 and §11.

---

## 4. Schema changes

Edit the existing `CREATE TABLE` statements in place (dev reset; no rebuild migration).

**`items`** (migration 1):

```sql
CREATE TABLE IF NOT EXISTS items (
    hk                    BLOB    NOT NULL,
    sk                    BLOB    NOT NULL DEFAULT x'',
    data                  ANY     NOT NULL,           -- TEXT | BLOB(bytes) | BLOB(JSONB)
    data_kind             INTEGER NOT NULL DEFAULT 0, -- 0=bytes, 1=text, 2=json
    ttl_epoch_utc_seconds INTEGER,
    v                     INTEGER NOT NULL,
    last_transaction_ts   INTEGER NOT NULL DEFAULT 0,
    est_row_bytes         INTEGER NOT NULL
        GENERATED ALWAYS AS (octet_length(data) + octet_length(hk) + octet_length(sk) + 44) STORED,
    PRIMARY KEY (hk, sk)
) WITHOUT ROWID, STRICT;
```

Notes:
- `data` stays `ANY` — we still physically mix TEXT and BLOB.
- `est_row_bytes` drops its `DEFAULT 0` and is now generated (was a plain column). It is no longer
  listed in any INSERT column list.
- `K = 44` ≈ prior `40` + the new `data_kind` integer column. Tune once with a smoke test.

**`pending_transactions`** (migration 2): add `data_kind INTEGER NOT NULL DEFAULT 0`. No generated
column here (transfer-size estimation only; see §7).

Verify before committing the schema (see §12): JSONB, `octet_length`, and a STORED generated column
are all accepted on the DO's SQLite build in a `WITHOUT ROWID, STRICT` table.

---

## 5. Write path

### 5.1 `db.ts` boundary — encode once

Add `encodeItemData` and call it in `putItem`, `transactWriteItems` (and any other data-carrying
entry point) right where keys are encoded:

```ts
function encodeItemData(data: string | Uint8Array | JsonComposite): EncodedItemData {
  if (data instanceof Uint8Array) return { kind: "bytes", data };
  if (typeof data === "string")   return { kind: "text", data };
  let text: string;
  try { text = JSON.stringify(data); }
  catch { throw new Error("fokos: data is not JSON-serializable"); }
  if (text === undefined) throw new Error("fokos: data serialized to undefined");
  return { kind: "json", data: text };
}
```

The RPC payload carries `{ kind, data }` instead of a bare `data`.

### 5.2 `PartitionStore.upsertItem` — conditional `jsonb()`, no JS size, RETURNING the generated size

```ts
upsertItem(opts: {
  hk: KeyBytes; sk: KeyBytes;
  data: string | Uint8Array; kind: DataKind;   // json ⇒ data is JSON text
  ttlEpochUtcSeconds: number | null; lastTransactionTs: number;
}): { version: number; keyEstBytes: number; rowsRead: number; rowsWritten: number } {
  const oldEst = this.#storage.sql
    .exec<{ est_row_bytes: number }>(`SELECT est_row_bytes FROM items WHERE hk = ? AND sk = ? LIMIT 1`, opts.hk, opts.sk)
    .toArray()[0]?.est_row_bytes ?? 0;

  const dataExpr = opts.kind === "json" ? "jsonb(?)" : "?"; // fixed fragment, not user input

  const writeRes = this.#storage.sql.exec<{ v: number; est_row_bytes: number }>(
    `INSERT INTO items (hk, sk, data, data_kind, ttl_epoch_utc_seconds, v, last_transaction_ts)
     VALUES (?, ?, ${dataExpr}, ?, ?, 1, ?)
     ON CONFLICT(hk, sk) DO UPDATE SET
       data = excluded.data,
       data_kind = excluded.data_kind,
       ttl_epoch_utc_seconds = excluded.ttl_epoch_utc_seconds,
       v = v + 1,
       last_transaction_ts = excluded.last_transaction_ts
     RETURNING v, est_row_bytes`,
    opts.hk, opts.sk, opts.data, DATA_KINDS.indexOf(opts.kind), opts.ttlEpochUtcSeconds, opts.lastTransactionTs,
  );
  // ... invariants on v ...
  const newEst = rows[0].est_row_bytes;         // exact, from the generated column
  // key_size_estimates delta uses newEst / oldEst exactly as today.
}
```

`data_kind` flows through `excluded.data_kind` on conflict. Metrics accounting is unchanged (still
the single items upsert statement).

### 5.3 `insertItemIfAbsent` (migration) — verbatim, drop the size arg

`StoredItemData` in; json `data` is the raw JSONB blob, bound directly (no `jsonb()`), `data_kind`
stored. The `estimateRowBytes(...)` argument is **removed** — the generated column computes it.

### 5.4 Transactions

- `insertPendingLock` gains `data_kind`, storing raw text/bytes (§2.8).
- `#applyCommitItems`/`upsertItem` call passes `kind` from the pending row.
- `TCWriteOperation`, `PendingTransactionRow`, `TransactionItem`, participant request types carry
  `kind` alongside `data`.

---

## 6. Read path

Two flavors:

**Public reads** (`getItem`, `queryItems`, `readForTransactionLocal`) decode json in SQL so JS never
touches raw JSONB:

```sql
SELECT hk, sk,
       CASE WHEN data_kind = 2 THEN json(data) ELSE data END AS data,  -- 2 = DATA_KINDS.indexOf("json")
       data_kind, ttl_epoch_utc_seconds, v, last_transaction_ts
FROM items ...
```

`fromSqlData` takes the `data_kind` code and returns `DecodedItemData`: `json` ⇒ value is JSON text
(returned as `{kind:"json", data:text}`), `text` ⇒ string, `bytes` ⇒ `Uint8Array`. The store maps the
code back with `DATA_KINDS[code]`. The `db.ts` boundary maps `json` → `JSON.parse` →
`JsonValue`, and surfaces `{ kind, data }` on the public result (§3, §11).

**Migration reads** (`queryItemsPage` used by migration) select `data` **without** the `CASE` and
return the raw JSONB blob for json rows (`StoredItemData`), for verbatim re-insert (§2.7, §5.3).

---

## 7. Size estimation — final state

- `estimateRowBytes`: **deleted** (generated column replaces it).
- `estimateItemBytes` / `estimatePendingTxBytes`: **kept unchanged.** They estimate rough
  transfer/batch size in JS, and since the DO only ever holds `string | Uint8Array`, the existing
  `typeof data === "string" ? data.length : data.byteLength` is correct (json rows are text or a
  JSONB blob at this layer — never a JS object). Exact storage bytes are intentionally not their job.
- `key_size_estimates` delta: kept, sourced from the generated `est_row_bytes` (§2.6).

---

## 8. Filtering / indexing (future — foundation only)

Enabled by `data_kind`, to be built in a later plan:

- Every JSON filter carries `WHERE data_kind = 2 AND jsonb_extract(data, '$.path') = ?` — the integer
  equality short-circuits before any JSON function runs on a bytes/text row.
- **Partial expression indexes** on json rows only:
  ```sql
  CREATE INDEX idx_items_json_status
    ON items (hk, jsonb_extract(data, '$.status')) WHERE data_kind = 2;
  ```
  Bytes/text rows add zero index bloat; the planner uses the index automatically.
- Whole-value string filters (equality/range/prefix/`LIKE`) run directly on TEXT rows — no JSON
  function needed.

---

## 9. In-place JSON mutation (future)

JSONB unlocks server-side attribute updates without a client read-modify-write:

```sql
UPDATE items SET data = jsonb_set(data, '$.status', jsonb(?)), v = v + 1
 WHERE hk = ? AND sk = ? AND data_kind = 2;
```

There is no true partial-blob write (SQLite rewrites the row; `sqlite3_blob_write` isn't exposed and
can't resize), but `jsonb_set`/`jsonb_insert`/`jsonb_replace`/`jsonb_remove`/`jsonb_patch` operate on
the binary form directly (no text parse/reserialize) and the generated `est_row_bytes` +
`key_size_estimates` delta keep bookkeeping correct. This is a capability **only json rows** have
(bytes are opaque, replace-only).

---

## 10. Future extensions

- **`ReadableStream` as input.** Accept a stream as `data` and consume it at the `db.ts` boundary
  into the corresponding kind (bytes, or text/JSON via incremental parse) before it reaches the DO.
  The DO contract stays `string | Uint8Array` + `kind`, so this is purely a boundary-side addition.
  Not in scope now.
- **Top-level JSON primitives.** Relax `JsonComposite` back toward `JsonValue` (accept top-level
  number/boolean/null) once we decide the semantics; additive, no schema change.
- **In-place updates / filtering** as above (§8, §9).

---

## 11. Resolved decisions

- **Public read results expose `kind`** alongside the reconstructed value (`DecodedItemData`,
  `{ kind, data }`), for symmetry with the kind-aware write path — callers switch on `item.kind`
  instead of `typeof`/`instanceof`/`Array.isArray` (§3, §6).
- `K` in the generated expression is just the fixed-overhead constant (integer columns + SQLite's
  per-record header). It's a rough tuning value, not a design decision — start at `44`.

---

## 12. Verification / prerequisites

Smoke-test on the DO's SQLite build **before** committing the schema:

1. JSONB is available: `SELECT hex(jsonb('{"a":1}'))` returns a blob.
2. `octet_length` is available and returns UTF-8 byte counts for TEXT and blob sizes for BLOB/JSONB.
3. A **STORED generated column** using `octet_length` is accepted inside a `WITHOUT ROWID, STRICT`
   table, and `RETURNING est_row_bytes` returns the generated value.
4. `json(data)` on a JSONB blob round-trips to the original text; a JSONB blob copied verbatim between
   two tables is still queryable with `jsonb_extract`.

Compat date (`2026-05-23`) is well past JSONB (3.45 / Jan 2024) and `octet_length` (3.43 / 2023), so
all four are expected to pass — but confirm rather than assume.

---

## 13. Test plan

- Round-trip each kind through put→get: bytes, text, and a nested object/array (JSON) — both the
  value and the `kind` exposed on the read result are preserved.
- `data_kind` persisted correctly; `est_row_bytes` equals `octet_length(data)+octet_length(hk)+
  octet_length(sk)+K` for each kind (esp. multi-byte UTF-8 text and JSON, where the old
  `.length` math diverged).
- `key_size_estimates` maintained across put / overwrite (kind change too) / delete, sourced from the
  generated column.
- Transaction commit of a json put stores JSONB and reads back the object.
- Migration copies a json row **verbatim** (JSONB blob unchanged) and the child can `jsonb_extract`
  it.
- `computeRangeSplitBoundaries` / `rebuildKeySizeEstimates` unaffected.
- Boundary rejects non-serializable / `undefined`-serializing data with a clear error.

---

## Appendix A — Future capabilities this `data_kind` + JSONB work unlocks

Comparison with the DynamoDB API (`AttributeValue`, `PutItem`) and the follow-on features our
storage model now enables. None of these are in scope for this plan; they are recorded here because
the `data_kind` discriminator and JSONB storage are their prerequisite.

### A.1 Type model: untyped document, not per-field tags (decided)

DynamoDB tags every value with a one-letter type (`S`, `N`, `B`, `BOOL`, `NULL`, `M`, `L`, and the
set types `SS`/`NS`/`BS`), all the way down a nested map. Those tags exist to escape JSON's type
system. We deliberately **do not** adopt per-field tagging: our `json` kind is an **untyped JSON
document**, queried on its natural shape (`$.user.age`, not `$.user.age.N`). Rationale: per-field
tags force every query/index path to speak the encoding and bloat the stored document — a bad trade
for a document store. The user may *choose* to store a DynamoDB-shaped object; we simply don't
privilege it.

Known gaps we accept vs. DynamoDB's tagged model (call out in docs, don't design in yet):
- **Number precision.** JSON numbers are IEEE-754 float64, and a JS number is already float64 before
  our boundary sees it — so large ints / high-precision decimals lose precision. DynamoDB avoids this
  by sending `N` as a **string** (38-digit decimal). Our guidance mirrors that rationale: if you need
  exact/large numbers in a `json` value, encode them as strings.
- **No native sets** (`SS`/`NS`/`BS`) — a set is a plain JSON array with app-level semantics.
- **No binary inside a document** — only the top-level `bytes` kind, or base64 text within JSON.
- Explicit **NULL vs. absent** we already get for free (a key with `null` vs. an absent key).

### A.2 `data_kind` as a versionable encoding discriminator (enabler)

`data_kind` is not just a value-type tag — it is an **encoding discriminator**, so new encodings can
live alongside `bytes`/`text`/`json` without disturbing them: e.g. a future `json_typed` kind that
stores a DynamoDB-`AttributeValue`-shaped document (full type fidelity: string-encoded numbers, sets,
in-document binary), or CBOR, or a compressed kind. This is the clean extension point that keeps the
untyped `json` kind simple while leaving a typed mode possible later.

### A.3 Condition / filter expression grammar (the primary follow-on)

Our current `ItemCondition` is a closed toy union (`item_exists`, `item_not_exists`,
`attribute_equals` on `v`). `data_kind` + JSONB is exactly what a real filter/condition layer needs.
Model it on DynamoDB's expression grammar, which maps almost 1:1 onto SQLite:
- functions `attribute_exists / attribute_not_exists / attribute_type / contains / begins_with / size`
  → `jsonb_extract(...) IS NOT NULL`, `json_type(...)`, `LIKE`/`instr`, `->> LIKE 'x%'`,
  `json_array_length` / `octet_length`.
- comparisons `= <> < > <= >= BETWEEN IN`, logical `AND / OR / NOT` → straight SQL.
- Adopt the **`#name` / `:value` placeholder indirection**: `#` names are sanitized into
  `jsonb_extract` JSON-paths (escaping / reserved words), `:` values become **bound SQL parameters**
  (injection-safe). Every JSON predicate is gated by `data_kind = <json>` so it never runs on a
  bytes/text row (see §8).

### A.4 `ReturnValues` on writes (enabler, cheap via `RETURNING`)

DynamoDB's `ReturnValues` (`ALL_OLD` / `ALL_NEW` / `UPDATED_*`) and
`ReturnValuesOnConditionCheckFailure` (`ALL_OLD`) are nearly free for us since writes already use
`RETURNING`. High-value cases: `ALL_NEW` for the future `jsonb_set` update path, and returning the
**current item on a failed condition** so optimistic-concurrency clients retry without an extra read.

### A.5 In-place attribute updates (see §9)

Server-side `jsonb_set` / `jsonb_remove` / `jsonb_patch` updates — a capability only `json` rows have
(bytes are opaque, replace-only) — combined with A.3 conditions and A.4 return values, is the
DynamoDB `UpdateItem` analog.

### A.6 Item-size limit + capacity/size primitives

DynamoDB caps items at 400 KB and exposes a `size` function; we already guard key sizes in `db.ts`
and now compute exact `est_row_bytes`, so a matching max-data-size guard is straightforward, and
`size` / `attribute_type` map to `octet_length` / `json_type` / `data_kind`. Our `meta`
(`rowsRead`/`rowsWritten`) is already the analog of DynamoDB's `ConsumedCapacity`.
