# RFC — A total order of GSI entries: a separator after the base sort key

**State:** Implemented
**Date:** 2026-11-22
**Author:** Lambros

## Table of contents

- [1. Overview and context](#1-overview-and-context)
- [2. Goals and requirements](#2-goals-and-requirements)
- [3. Milestones](#3-milestones)
- [4. Proposed solution](#4-proposed-solution)
- [5. Alternative options](#5-alternative-options)
- [6. Frequently asked questions](#6-frequently-asked-questions)
- [7. References](#7-references)

## 1. Overview and context

### 1.1 The problem

A GSI partition stores each entry with a physical sort key (`shared/gsi/sort-key.ts`):

```
0xFF  escape(gsiSK)  00 00  escape(baseHK)  00 00  escape(baseSK)  [rawGsiSK]  <L> <flag> <D>
```

`escape` writes each `0x00` byte as `0x00 0xFF`. The trailer `<L><flag><D>` holds the byte length of `gsiSK`.

`gsiSK` and `baseHK` end with the separator `00 00`. `00 00` is less than each byte that can follow it, so these two
parts sort in the byte order of their values. `escape(baseSK)` ends with the trailer, and the trailer starts with an
ASCII digit (`0x30` to `0x39`). Thus a base sort key that is a prefix of another base sort key can sort after it.

An example with `gsiSK = "open"` and `baseHK = "u1"`. The trailer is `4 : 1`. The rows are in the stored order:

```
baseSK         stored sort key (·xx is one byte in hex)
"order#0100"   ·ff o p e n ·00 ·00 u 1 ·00 ·00 o r d e r # 0 1 0 0 4 : 1
"order#01"     ·ff o p e n ·00 ·00 u 1 ·00 ·00 o r d e r # 0 1 4 : 1
"order"        ·ff o p e n ·00 ·00 u 1 ·00 ·00 o r d e r 4 : 1
```

The byte order of the values is `"order"`, `"order#01"`, `"order#0100"`. The stored order is the reverse, because `#`
(`0x23`) and `0` (`0x30`) are less than the trailer byte `4` (`0x34`).

Binary keys have the same defect. A binary key is `0xFF` and its bytes, and any byte can follow the prefix:

| `baseSK` values | Stored order today | Byte order of the values |
|---|---|---|
| `[01]`, `[01 01]` | `[01 01]` first | `[01]` first |
| `[01]`, `[01 00]` | `[01 00]` first (`00 FF` is less than `34`) | `[01]` first |

### 1.2 Why it must be solved now

The `scanItems` RFC (`2026-11-20-scan-items.md`, section 4.2.9) builds a GSI resume key from a range boundary. It
assumes that a cut prefix, with the parts after the cut absent, sorts at or below each entry that starts with that
prefix. The trailer breaks this assumption. A scan can then return a resume key that is after the next range boundary
`S`, and the next page skips the entries between `S` and the key. For example, with the boundaries
`a = … "order#01"` and `S = … "order#0101"`, the key `… "order#01" 4 : 1` is after `S`, and also after the entry
`"order#0101"`.

### 1.3 What the reader must know about the current system

- `encodeGsiSortKey` builds the physical key. `gsiItemSortKey` adds the binary tag `0xFF`. `decodeGsiSortKey` reads
  the trailer from the end, finds the end of the body, and splits the body at each `00 00`. It refuses a flag other
  than `:` and `!`.
- SQL reads the GSI sort key with `substr` (`GSI_SORT_KEY_VALUE` in `shared/expression/compiler.ts`). It reads `<D>`,
  `<flag>`, and `<L>` from the end, and `gsiSK` from byte 2 (flag `:`) or from the raw copy before `<L>` (flag `!`).
  It reads each flag other than `!` as `:`. `GSI_SORT_KEY_PRESENT` reads bytes 2 and 3.
- `gsiSkInterval` (`shared/gsi/query.ts`) bounds a GSI query with `0xFF escape(k) 00 00` and `0xFF escape(k) 00 01`.
  It reads only the part before the first separator.
- `gsiLimits` (`shared/partition-context.ts`) gives `maxSortKeyBytes` of a GSI table. It counts two separators.
- A range split computes each boundary with `KeyCodec.shortestCompositeSeparator` (`split-planner.ts`). The boundary
  is the shortest prefix of the upper key that is above the lower key. It can cut a GSI sort key at any byte. The
  sharding code knows nothing about the GSI layout.
- FokosDB is not released. A change of the stored layout needs no data migration.

## 2. Goals and requirements

### 2.1 In scope

- The stored order of GSI entries is the order of `(gsiSK, baseHK, baseSK)`, each part compared as `KeyBytes` with
  `KeyCodec.compare`. This holds for string keys, binary keys, absent keys, and keys with `0x00` bytes.
- A prefix of the stored key, cut in any part and completed with the smallest valid continuation, sorts at or below
  each stored key that starts with it. Section 4.2.9 of the `scanItems` RFC then holds as it is written.
- `decodeGsiSortKey(encodeGsiSortKey(g, h, s))` gives back `(g, h, s)` for each valid input.
- The entry prefix (section 4.2) has one definition in code, and the decoder and the tests use it.
- A range boundary between two GSI entries ends inside the entry prefix of the upper entry (section 4.3).
- The flag byte identifies the layout (section 4.4).

### 2.2 Out of scope

- **Data in the old layout.** FokosDB is not released. A deployment with GSI data must destroy and build its GSIs
  again. The decoder does not read the old layout.
- **The order of base items in a public API.** DynamoDB does not specify the order of entries with the same GSI keys,
  and FokosDB keeps this rule. This RFC fixes the internal order. It adds no public promise.
- **A change of the trailer or of the escape.** Both stay as they are.
- **An online change of the layout.** Entries of two layouts in one GSI sort in no useful order. A new layout needs a
  GSI that is built again, or a new GSI shard group with a backfill. The flag of section 4.4 only identifies a layout.
- **A check of GSI boundaries in production code.** The split code knows nothing about the GSI layout. Section 4.3
  proves the property, and a test checks it.

### 2.3 Requirements

- The SQL expressions in `compiler.ts` do not change. Each one reads from the end, or reads bytes 2 and 3.
- The GSI query bounds in `query.ts` do not change.
- The size of a stored entry grows by exactly 2 bytes.
- The split code in `sharding/` and `split-planner.ts` does not change.

## 3. Milestones

1. **Codec and limits.** The new layout in `encodeGsiSortKey` and `decodeGsiSortKey`, `gsiEntryPrefixLength`, the
   new `gsiLimits`, and the tests of section 4.6. The full test suite passes.
2. **Documentation.** In `2026-10-17-coalesced-materialization-and-gsis.md`:
   - Section "Order-preserving composite sort key with `<L><flag><D>` trailer": the "Join" bullet ends each of the
     three parts with `0x00 0x00`. The byte positions of the two diagrams ("Case 1" and "Case 2") include the third
     separator. The property "Contiguous `gsiSK` ranges" states the total order of section 4.2, and not the order of
     `escape(baseSK) || trailer`.
   - Section "Derived GSI key limits": `4` becomes `6`, "two separators" becomes "three separators", and 4,618 bytes
     becomes 4,620 bytes.

   In section 4.2.9 of `2026-11-20-scan-items.md`, add the note of section 4.3 to case 3. Its layout sentence already
   shows the new layout.

## 4. Proposed solution

### 4.1 The new layout

Add the separator `00 00` after `escape(baseSK)`. The raw copy and the trailer follow it:

```
0xFF  escape(gsiSK)  00 00  escape(baseHK)  00 00  escape(baseSK)  00 00  [rawGsiSK]  <L> <flag> <D>
└──────────────────────────────── entry prefix ──────────────────────────┘ └──────── suffix ─────────┘
```

The example of section 1.1 in the new layout, in the stored order:

```
baseSK          stored sort key
"order"         ·ff o p e n ·00 ·00 u 1 ·00 ·00 o r d e r ·00 ·00 4 : 1
"order#01"      ·ff o p e n ·00 ·00 u 1 ·00 ·00 o r d e r # 0 1 ·00 ·00 4 : 1
"order#0100"    ·ff o p e n ·00 ·00 u 1 ·00 ·00 o r d e r # 0 1 0 0 ·00 ·00 4 : 1
bin [01]        ·ff o p e n ·00 ·00 u 1 ·00 ·00 ·ff ·01 ·00 ·00 4 : 1
bin [01 00]     ·ff o p e n ·00 ·00 u 1 ·00 ·00 ·ff ·01 ·00 ·ff ·00 ·00 4 : 1
bin [01 01]     ·ff o p e n ·00 ·00 u 1 ·00 ·00 ·ff ·01 ·01 ·00 ·00 4 : 1
```

### 4.2 Why the order is total

Each part ends with `00 00`, and `00 00` never occurs inside an escaped part, because `escape` writes each `0x00` as
`00 FF`. Compare two stored keys, part by part:

1. When two parts are equal, both continue with `00 00`, and the comparison goes to the next part.
2. When one part is a strict prefix of the other part, the shorter part continues with `00 00`. The longer part
   continues with a byte from `01` to `FF`, or with `00 FF` for a `0x00` byte of its value. `00` is less than each
   byte from `01` to `FF`, and `00 00` is less than `00 FF`. So the shorter part sorts first, as in `KeyCodec.compare`.
3. When the two parts differ at a byte, the escape keeps the order. A `0x00` byte becomes `00 FF`, and this is less
   than any byte from `01` to `FF`.

String keys and binary keys use the same rules. A string key has no `0x00` byte (`validateKeyContent`), so the escape
does not change it. A binary key starts with `0xFF`, and UTF-8 never holds `0xFF`, so each string part sorts before
each binary part, as in the base table.

**The entry prefix and the suffix.** The entry prefix is the bytes from the start to the end of the third separator.
It identifies the entry. The suffix is the raw copy and the trailer. The suffix is a function of `gsiSK` only, so two
keys with the same entry prefix are equal.

Two different entries differ in one of the three parts, so the comparison ends inside the entry prefix. The suffix
never decides the order. Two equal entries have equal bytes, so the key stays unique.

### 4.3 Range boundaries end inside the entry prefix

A range split computes the boundary `b = shortestSeparator(lo, hi)`, with `lo < hi`. `b` is `hi[0..i+1]`, where `i`
is the first byte where `lo` and `hi` differ. When `lo` is a prefix of `hi`, `b` is `hi[0..lo.length+1]`.

For two stored GSI sort keys `lo < hi`, `i` is inside the entry prefix of `hi`:

1. `lo` and `hi` are different entries, so section 4.2 decides their order at a byte inside both entry prefixes.
2. `lo` is never a prefix of `hi`. When it is, the entry prefix of `lo` is a prefix of `hi`. The third separator of
   `lo` is then also the third separator of `hi`, so both keys have the same entry prefix and are equal.

Thus `b.length` is at most the length of the entry prefix of `hi`, with the tag byte. A boundary never cuts inside the
raw copy or the trailer. A boundary with two hash keys has the empty sort key, which is also inside the entry prefix.

Case 3 of section 4.2.9 of the `scanItems` RFC (a cut inside the raw copy or the suffix) thus never occurs for a
boundary from a range split. The client keeps case 3, because it costs few lines and gives a correct result for each
input. The `scanItems` RFC states that a range split does not produce this case.

This RFC adds no check to the split code. The split code knows nothing about the GSI layout, and AGENTS.md forbids
a production hook for a test. A property-based test checks the property (section 4.6).

### 4.4 The flag identifies the layout

The flag byte also identifies the layout of the key. The rules:

- A new layout uses new flag values. A flag value is never an ASCII digit.
- `decodeGsiSortKey` refuses an unknown flag. It does this today, and the new layout keeps the two values `:` and `!`.
- `GSI_SORT_KEY_VALUE` reads each flag other than `!` as `:`. Thus a new flag value also changes this expression.
  The comment above `GSI_D` in `compiler.ts` states this rule.

The suffix never decides the order (section 4.2), and SQL reads the flag from the end. Thus the flag identifies the
layout with no extra byte, no change of the order, and no change of the `substr` positions.

The flag only identifies a layout. It does not make an online change of the layout possible (section 2.2). When the
code meets a key of an unknown layout, the decode fails with a clear error, and the operator builds the GSI again.

The flag does not identify the current layout, which this RFC replaces. That layout has the same flag values `:` and
`!`. Its body does not end with `00 00`, or it has only two separators before the end. Thus the decode of a key in the
current layout fails on the check of the entry prefix (section 4.5), and not on the flag.

### 4.5 Code changes

- **`encodeGsiSortKey`** writes `SEPARATOR` after `escapeKey(baseSK)`, and before the raw copy and the trailer. Update
  its comment and the layout comment at the top of `sort-key.ts`. The layout comment names the entry prefix, the
  suffix, and the rule of section 4.4.
- **`gsiEntryPrefixLength(physicalSk)`** is a new export of `sort-key.ts`. It reads the trailer from the end and
  returns the length of the entry prefix: the length of the body, with the third separator. It takes the bytes without
  the tag, as `decodeGsiSortKey` does. It throws on a trailer that is not valid. Steps 1 to 3 of `decodeGsiSortKey`
  move into it.
- **`decodeGsiSortKey`** gets the entry prefix length from `gsiEntryPrefixLength`. The entry prefix must end with
  `00 00`. The decoder checks this with an `invariant`, and splits only the bytes before it into the three parts.
- **`gsiLimits`** counts three separators: `1 + 2 * (2 * s + h) + 6 + s + String(s).length + 2`. With the default
  limits (`h = 1024`, `s = 512`), `maxSortKeyBytes` is 4,620 bytes. Update its comment.
- **`compiler.ts`**: the SQL does not change. Update the layout comment above `GSI_D`, and add the rule of
  section 4.4.
- **`query.ts`**, the split code, and the callers of `gsiItemSortKey` (`target-runner.ts`, `do-partition.ts`) need
  no change.

### 4.6 Testing

In `shared/gsi/sort-key.test.ts`:

- Update the expected bytes of the layout tests: `"2026-10-17\0\0u1\0\0o9\0\x0010:2"`, the `!` case, and the case
  with no GSI sort key, `"\0\0u1\0\0\0\x000:1"`. A `\0` before a digit is an octal escape, which strict mode
  refuses, so the tests write `\x00` there.
- **Prefix order.** For string and binary base sort keys, a value sorts before each longer value that starts with it.
  Include the cases of section 4.1, and the same cases for `gsiSK` and `baseHK`.
- **Property-based total order.** For random triples, with string, binary, and absent parts, `0x00` bytes, shared
  prefixes, and bytes around `0x00`, `0x01`, `0x23`, `0x30`, `0x3A`, and `0xFF`:
  `sign(compare(encode(x), encode(y)))` equals the sign of the part-by-part `KeyCodec.compare` of `x` and `y`. Read
  `test/property-based/README.md` first. A prototype of this check found 29 mismatches in 200,000 pairs with the
  current layout, and none with the new layout.
- **Entry prefix.** For random triples, `gsiEntryPrefixLength(k)` is the position after the third separator of `k`.
  Two keys with the same entry prefix are equal. The bytes of the entry prefix decide each comparison of two
  different keys: the first byte where two keys differ is inside both entry prefixes.
- **Boundaries.** For random pairs `lo < hi` of stored GSI sort keys (with the tag),
  `KeyCodec.shortestSeparator(lo, hi).length` is at most `1 + gsiEntryPrefixLength(hi without the tag)`. Use the
  generator of the total-order test.
- **Round trip.** The current round-trip cases pass with the new layout.
- **Bad input.** A body that does not end with `00 00` fails to decode. A key in the current layout fails to decode,
  with a base sort key (`"a\0\0b\0\0c1:1"`) and without one (`"a\0\0b\0\x001:1"`). A flag other than `:` and `!` fails to decode,
  also in `gsiEntryPrefixLength`.

In the GSI suites, the SQL filters on `{ ref: "sortKey" }` and the GSI queries pass without a change.

## 5. Alternative options

- **Put the trailer first, before `escape(gsiSK)`.** Rejected: the trailer would then decide the order before the
  GSI sort key, and the GSI query bounds would break.
- **A special resume key in `scanItems` for a cut inside the base sort key**, for example a value below `S`.
  Rejected: it needs new cases and a fallback, and it fixes only the scan. The stored order stays wrong.
- **Accept the order and document it.** Rejected: every key-only resume position on a GSI would need special rules.
- **The separator `00 01`, as in CockroachDB.** Rejected: it gives the same order. With `00 00`, the smallest
  continuation of a part is a valid key. The upper bound `escape(k) 00 01` of `gsiSkInterval` also needs `00 01`
  between the separator and the escaped NUL `00 FF`.
- **A layout byte after the tag `0xFF`**, as the index ID in a CockroachDB key. Rejected: the byte would take part in
  the order, and each `substr(i.sk, 2, …)` in `compiler.ts` would move. A new GSI shard group gives the same
  separation of two layouts.
- **Cut each GSI boundary to the entry prefix in the split code**, as `EnsureSafeSplitKey` in CockroachDB. Rejected:
  section 4.3 proves that each boundary already ends inside the entry prefix, and the split code would need to know
  the GSI layout.

## 6. Frequently asked questions

**Does this change the order of `queryItems` results on a GSI?**
Only for entries with the same GSI sort key and the same base hash key, when one base sort key is a prefix of another.
No public contract defines that order.

**Why does the separator not fail for a binary key with `0x00` bytes?**
The escape writes each `0x00` byte as `00 FF`. Thus `00 00` occurs only as a separator, and section 4.2 holds for
each byte value.

**What does the change cost?**
Two bytes for each GSI entry. No extra row, no extra read, and no change to the SQL.

**Can a future layout change use the flag to migrate a GSI online?**
No. Entries of two layouts in one GSI sort in no useful order. The flag only makes the decode of a future layout fail
with a clear error. A key in the current layout fails on the check of the entry prefix (section 4.4). An online change needs a new GSI shard group and a backfill.

## 7. References

- `packages/fokosdb/src/shared/gsi/sort-key.ts`
- `packages/fokosdb/src/shared/gsi/query.ts`
- `packages/fokosdb/src/shared/expression/compiler.ts` (`GSI_D`, `GSI_L`, `GSI_SORT_KEY_VALUE`, `GSI_SORT_KEY_PRESENT`)
- `packages/fokosdb/src/shared/partition-context.ts` (`gsiLimits`)
- `packages/fokosdb/src/shared/partition/split-planner.ts` and `packages/fokosdb/src/sharding/key-codec.ts`
  (`shortestSeparator`, `shortestCompositeSeparator`)
- `docs/agent-plans/2026-10-17-coalesced-materialization-and-gsis.md`
- `docs/agent-plans/2026-11-20-scan-items.md`
- CockroachDB `pkg/util/encoding` (the key encoding of bytes) and `pkg/keys` (`MakeFamilyKey`,
  `GetRowPrefixLength`, `EnsureSafeSplitKey`)
