# RFC — Unambiguous and readable partition DO names

**State:** Implemented  
**Date:** 2026-09-27  
**Author:** Lambros Petrou

**Status:** Implemented: the text encoding of the range components, the `~` shard group separator, and the
`fokos.p.` partition shard group. Milestone 4 is a release step: it deletes the Worker and all its Durable Objects
before it deploys the new version.

## Table of contents

- [1. Overview and context](#1-overview-and-context)
- [2. Goals and requirements](#2-goals-and-requirements)
- [3. Milestones](#3-milestones)
- [4. Proposed solution](#4-proposed-solution)
  - [4.1 High-level overview](#41-high-level-overview)
  - [4.2 Technical details](#42-technical-details)
- [5. Alternative options](#5-alternative-options)
- [6. Frequently asked questions](#6-frequently-asked-questions)
- [7. References](#7-references)
- [8. Appendix: encoded lengths for 20 common key formats](#8-appendix-encoded-lengths-for-20-common-key-formats)

## 1. Overview and context

A range partition has the Durable Object name `<shardGroup>.r.<hk>.<start>.<end>`.
`rangePartitionDoName` in `packages/fokosdb/src/sharding/partition-id.ts` builds it.
`getByName` hashes the name to find the Durable Object, so the name is the address of the partition data.
Operators also read the name in logs (`actorName`), in destroy warnings, in context-mismatch errors,
and in the Cloudflare dashboard.

Today, `encodeRangeComponent` percent-encodes each of the three components byte by byte.
A byte in the safe set (`isSafeNameByte`) stays literal. Each other byte becomes `%XX`.
This is readable and short for ASCII keys. It is three characters per byte for all other bytes:

- A binary key (`0xFF` tag) becomes a string of `%XX` escapes that no operator can read.
  A 16-byte binary UUID uses 39 characters. Its base64url form uses 23.
- A non-English string key becomes `%XX` escapes for each non-ASCII byte.
  `議事録_2024年3月.docx` uses 58 characters. Its text form uses 18.

A range boundary is a byte prefix of a real sort key: `KeyCodec.shortestSeparator` returns the shortest prefix
of the upper key that is greater than the lower key.
Thus a boundary of an ASCII key is ASCII, and a boundary of a multi-byte UTF-8 key can end
in the middle of a character. Section 8 has the data for 20 common key formats.

A hash partition has the Durable Object name `<shardGroup>.h.<root>[.<child>...]`.
`PartitionIdHelper.doName` builds it. Both name formats put the shard group first, without an escape.
`validateTopology` requires only that `shardGroup` is a non-empty string, and a table name becomes the shard group
(`partition-context.ts`). Thus a shard group can contain `.`, and two partitions of two tables can get one name:

- Table `x.r.k`, hash root 0: `x.r.k.h.0`.
- Table `x`, range partition with hash key `k`, start `h`, and end `0`: `x.r.k.h.0`.

The two partitions then share one Durable Object and its data.
A fixed prefix for all shard groups, such as `fokos.p.`, does not prevent this collision:
`fokos.p.x.r.k` + `.h.0` and `fokos.p.x` + `.r.k.h.0` give the same name.

`docs/agent-plans/2026-09-26-range-partition-id-base64url.md` changes the range partition ID and keeps the DO name.
This RFC changes the DO names only.

## 2. Goals and requirements

### 2.1 In scope

- A component of a binary key (first byte `0xFF`) must encode as `~b` + unpadded base64url of all its bytes.
- A component of a string key must encode as its decoded text,
  with `%XX` escapes only for a fixed set of characters and for the bytes of an incomplete trailing UTF-8 sequence.
- An ASCII string component must encode as it does today.
- A null boundary must continue to encode as `~min` or `~max`.
- The names must use `~` after the shard group: `<shardGroup>~h.<root>[.<child>...]` and
  `<shardGroup>~r.<hk>.<start>.<end>`.
- `validateTopology` must reject a `shardGroup` that contains `~`.
- The shard group of the partitions of a table must be `fokos.p.<tableName>`.
  The shard group of its transaction coordinators stays `fokos.tc.<tableName>`.
- Two partitions of two different shard groups must never get the same name.

### 2.2 Out of scope

- The hash partition name after the `~` separator does not change: `h.` and the root and child indexes.
- The range partition ID does not change. `docs/agent-plans/2026-09-26-range-partition-id-base64url.md` owns it.
- Compatibility with the Durable Objects of existing partitions is out of scope.
  Section 4.2 describes which names change, and the deployment that this change needs.
- Detection of confusable characters (for example, Cyrillic `а` for Latin `a`) is out of scope.
  It needs Unicode tables, and the name rules must not depend on them.

### 2.3 Requirements

- The encoding must be deterministic: the same component bytes must always give the same name component.
- The encoding must be injective: two different component byte strings must give two different name components.
- The encoded component must not contain `.`, because `.` separates the components.
- The shard group must not contain `~`, because the first `~` of a name ends the shard group.
- The encoded component must not start with `~` unless it is `~min`, `~max`, or a `~b` base64url component.
- The escape set must be a fixed list of code points in the code.
  It must not use Unicode properties or categories, such as `\p{C}` or `\p{Z}`.
- The escape set, the `~b` marker, the base64url alphabet, the tail rule, and the `~` separator must not change
  after release.
  A change to any of them changes the Durable Object of a partition and makes its data unreachable.
- The decoder must keep a leading U+FEFF. A decoder that removes it makes two keys give one name.
- The name must not have a length limit. `getByName` hashes the name, so its length does not affect routing.
- The name can hold non-ASCII characters. Production `getByName` accepts them.
- The code must use `ctx.id.name` only for logs and error attributes. The runtime decodes the name for
  `ctx.id.name`, and the result can differ from the string that `getByName` received.
  Routing and identity checks must use the `doName` of the route context or of the stored identity.

## 3. Milestones

1. Change `encodeRangeComponent` and its tests. The component encoding of each ASCII key stays the same.
2. Add the `~` separator after the shard group in the hash and range names, and the `~` check in
   `validateTopology`. Update the tests that hold fixed names.
3. Add the `fokos.p.` prefix to the shard group of the partitions of a table.
   Keep the coordinator shard group `fokos.tc.<tableName>`. Update the tests that hold fixed names.
4. Release milestones 1, 2, and 3 in the same release as the range partition ID change.

## 4. Proposed solution

### 4.1 High-level overview

Each of the three name components (hash key, start boundary, end boundary) goes through one encoder.
The encoder picks one of three forms from the component bytes alone.

```text
component bytes
  |-- null boundary                        --> "~min" or "~max"   (unchanged)
  |-- first byte 0xFF (binary key)         --> "~b" + base64url(all bytes)
  `-- string key
        |-- split off an incomplete UTF-8 tail (at most 3 bytes, only at the end)
        |-- decode the rest as UTF-8 (fatal)
        |     `-- decode fails             --> "~b" + base64url(all bytes)
        |-- escape the fixed character set as %XX of its UTF-8 bytes
        `-- append %XX for each tail byte

"photos/2024/03/15/IMG_20240315_143"  --> photos/2024/03/15/IMG_20240315_143   (same as today)
"john.doe+"                           --> john%2Edoe+                          (same as today)
"議事録_2024年3月.docx"                 --> 議事録_2024年3月%2Edocx
boundary cut inside 録                 --> 議事%E9
16-byte binary UUID                   --> ~b__R6wQtYzENypWcOArLD1Hk
```

An ASCII key gives the same components as today, because the ASCII part of the escape set is the current unsafe
byte set. A non-English key gives readable text. A binary key gives base64url, which uses 4 characters for each
3 bytes.

A `~` after the shard group separates it from the rest of the name. A shard group cannot contain `~`,
so the first `~` of a name always ends the shard group:

```text
<shardGroup>~h.<root>[.<child>...]        fokos.p.orders~h.3.1
<shardGroup>~r.<hk>.<start>.<end>         fokos.p.orders~r.USER#jo.~min.photos/2024
```

### 4.2 Technical details

#### Encoder steps

`encodeRangeComponent(bytes)` does these steps:

1. When `bytes[0]` is `0xFF`, return `~b` + `bytes.toBase64({ alphabet: "base64url", omitPadding: true })`.
2. Find the incomplete tail. Examine the last 3 bytes from the end, and find the last byte that is not a
   continuation byte (`b & 0xC0 !== 0x80`). When it is a lead byte and the bytes from it to the end are fewer
   than its sequence length, the tail starts at that lead byte. Otherwise the tail is empty.
   The sequence length is 2 for `0xC0`–`0xDF`, 3 for `0xE0`–`0xEF`, and 4 for `0xF0`–`0xF7`.
   When the last 3 bytes are all continuation bytes, or the byte found is ASCII or another value,
   the tail is empty. Step 3 then handles the bytes.
3. Decode the bytes before the tail with one shared `TextDecoder("utf-8", { fatal: true, ignoreBOM: true })`.
   When the tail is empty, decode `bytes` directly. Make a `subarray` view only when there is a tail.
   When the decoder throws, return `~b` + base64url of all the bytes.
4. Test the decoded text with a regular expression of the escape set. When the test finds none, the text is
   the result. Otherwise, scan the text one code unit at a time. Replace each character of the escape set with
   `%XX` for each of its UTF-8 bytes, and append the text between two escapes as one slice.
   The hex digits are uppercase, and they come from a table of the 256 `%XX` strings.
   A loop at module scope builds the table once, when the module loads. The source does not hold the 256 strings.
5. Append `%XX` for each tail byte.

A string key is valid UTF-8, because `KeyCodec.encode` rejects a string that is not well-formed UTF-16.
A boundary is a byte prefix of a string key, so only its last 1 to 3 bytes can be an incomplete sequence.
When a component ends with a complete 4-byte character, such as `a🎉` (`61 F0 9F 8E 89`), its last 3 bytes are
all continuation bytes. Step 2 then gives an empty tail, which is correct, and step 3 decodes the character.
Thus the empty-tail case of step 2 is a normal path for string keys, and the code must not treat it as an error.
Only the fallback of step 3 occurs for bytes that no string key gives. It keeps the encoder total.
Step 2 reads at most 3 bytes, and step 3 makes a `subarray` view only when there is a tail.

A string key never starts with `0xFF`, because `0xFF` is not valid in UTF-8. Thus step 1 selects binary keys only.

#### Escape set

The escape set is the following fixed list. The code must hold it as literal code point ranges.

| Code points | Characters | Reason |
| --- | --- | --- |
| U+0000–U+0020 | C0 controls and space | Log lines and readability. Escaped today. |
| U+0022 | `"` | JSON and log output. Escaped today. |
| U+0025 | `%` | The escape character. Escaped today. |
| U+002E | `.` | The component separator. Escaped today. |
| U+005C | `\` | JSON and log output. Escaped today. |
| U+007E | `~` | Reserved for `~min`, `~max`, `~b`, and the shard group separator. Escaped today. |
| U+007F | DEL | Control character. Escaped today. |
| U+0080–U+009F | C1 controls | Control characters. |
| U+200B–U+200F | Zero-width space, zero-width joiners, LRM, RLM | Invisible in logs. |
| U+2028–U+2029 | Line separator, paragraph separator | They break log lines. |
| U+202A–U+202E | Bidirectional embeddings and overrides | They reorder the text on screen. |
| U+2066–U+2069 | Bidirectional isolates | They reorder the text on screen. |
| U+FEFF | Byte order mark, zero-width no-break space | Invisible in logs. |

The ASCII rows are exactly the bytes that `isSafeNameByte` rejects today.
The escape set uses fixed ranges because a Unicode category can change when V8 updates its Unicode version.
A changed category changes the name, and a changed name gives a different Durable Object.

#### Injectivity

The encoder is injective for these reasons:

- The text form never contains `~`, because the escape set holds `~`.
  The base64url alphabet has no `~`. Thus a `~b` component never equals a text component.
- The base64url form encodes all the bytes, including the `0xFF` tag. Different bytes give different base64url.
- In the text form, `%` is always escaped. Thus each `%XX` in the output stands for exactly one input byte.
- Each other character in the text form stands for its own UTF-8 bytes.
  A reader can parse the text form back into the original bytes. Two different inputs cannot give one output.
- The decoder uses `ignoreBOM: true`, so it keeps a leading U+FEFF, and step 4 escapes it.
- `~min` and `~max` start with `~m`. No other form starts with `~m`.

#### Shard group separator

`rangePartitionDoName` and `PartitionIdHelper.doName` write `~` after the shard group, where they write `.` today.
The rest of each name does not change: `h.` and the indexes, or `r.` and the three components.
`validateTopology` rejects a `shardGroup` that contains `~`, with the `partition_context_options_invalid` code.
The check is in the sharding layer, so it also applies to a caller that uses the sharding entry points directly.

Two names of two different partitions are always different, for these reasons:

- The shard group has no `~`, so the first `~` of a name ends the shard group.
  Two equal names thus have the same shard group.
- After the `~`, the kind is `h` or `r`, so a hash name never equals a range name.
- A hash name has only decimal indexes after `h.`. The indexes follow from the partition ID.
- A range name has exactly three components after `r.`, and no component contains `.`.
  The component encoding is injective.
- A `~` after the first `~`, in `~min`, `~max`, or `~b`, does not end the shard group,
  because only the first `~` does.

The internal shard group prefixes keep their dots: `fokos.tc.` of the transaction coordinators (`db.ts`),
and `fokos.p.` of the partitions (milestone 3). A DynamoDB table name cannot contain `~`, because DynamoDB permits
only `[a-zA-Z0-9_.-]` in table names.

With the `~` separator, the example of section 1 gives two names:

- Table `x.r.k`, hash root 0: `x.r.k~h.0`.
- Table `x`, range partition with hash key `k`, start `h`, and end `0`: `x~r.k.h.0`.

#### Partition shard group prefix

`FokosDbPartitionContext` in `partition-context.ts` sets the partition shard group to `fokos.p.<tableName>`.
Today it uses the table name alone. The coordinator shard group stays `fokos.tc.<tableName>`.

Two lines of code depend on the shard group of the partitions, and the change must update both:

- `partition-context.ts` rejects a table name that starts with `fokos.`, because FokosDB reserves that prefix.
  Today the check reads `topology.shardGroup`. With the prefix, that value always starts with `fokos.`,
  so the check must read the table name.
- `db.ts` builds the coordinator shard group as `fokos.tc.<topology.shardGroup>`.
  With the prefix, that gives `fokos.tc.fokos.p.<tableName>`, so the code must use the table name.

For the table `orders`, the names become:

```text
fokos.p.orders~h.3.1                 partition, hash
fokos.p.orders~r.USER#jo.~min.~max   partition, range
fokos.tc.orders~h.7                  transaction coordinator
```

The prefix does not prevent the collision of section 1. The `~` separator prevents it.
The prefix makes all shard groups of FokosDB start with `fokos.`, so the kind of each Durable Object
is clear from its name.

#### Which names change

The `~` separator changes the name of each partition, hash and range. It also changes the name of each
transaction coordinator, because the coordinator client builds hash names with `PartitionIdHelper` (`router.ts`)
for the shard group `fokos.tc.<tableName>` (`db.ts`). A `clientRequestToken` selects a coordinator by its name,
so a replay of a transaction from before the release reaches a new, empty coordinator.
The prefix of milestone 3 changes the name of each partition again (section "Partition shard group prefix").
The component encoding changes these range name components:

| Component bytes | Component today | Component after the change |
| --- | --- | --- |
| ASCII string key | percent-encoded | the same string |
| String key with a byte ≥ `0x80` | percent-encoded | different: text with escapes |
| Binary key | percent-encoded | different: `~b` + base64url |
| Null boundary | `~min` or `~max` | the same string |

Each partition and each transaction coordinator gets a new Durable Object.
Its data stays in the old Durable Object, and no request reaches it after the change.
The range partition ID change breaks the stored identity of every range partition. Thus this change must ship
in the same release, so that the deployment has one breaking change for range partitions and not two.

During a deployment, the old and the new version of a Worker can serve requests at the same time.
Then two versions can send the writes of one range to two different Durable Objects.
Thus the release must not roll out over existing data. The deployment deletes the Worker and all its
Durable Objects, in the partition namespace and in the transaction coordinator namespace,
and then deploys the new version. This release has no data migration.

#### Performance

The encoder runs when a partition builds a range target, which includes a learned range target on a request.

`packages/fokosdb/test/do-names-bench/range-do-name.workerd-bench.ts` measures the encoder inside workerd.
`pnpm --filter fokosdb bench:workerd` runs it through `@cloudflare/vitest-plugin`, and `pnpm test` does not run it.
The inputs are the 40 components of section 8: each key, and its boundary.
The benchmark puts each component in one of three groups:

- No escapes: text with no character of the escape set and no incomplete tail. 24 components, 26 bytes average.
- With escapes: text with a character of the escape set or an incomplete tail. 14 components, 36 bytes average.
- Binary: binary keys. 2 components, 11 bytes average.

Timers in workerd advance in whole milliseconds. Thus each value is the median of 7 batches,
and each batch takes at least 200 ms. The table shows the range of two runs, in ns per component.
A "Decode" row decodes the whole component, so it measures only the components with no incomplete tail.

| Operation | No escapes | With escapes | Binary |
| --- | ---: | ---: | ---: |
| Percent-encoding, today's loop | 254–260 | 467–471 | 376–387 |
| Percent-encoding with the hex table | 244–260 | 299–325 | 85–90 |
| base64url (`toBase64`) | 141–147 | 137–141 | 135–149 |
| Decode only | 134–135 | 144–146 | – |
| Decode, then a test | 135 | 155–164 | – |
| Decode, then a regular-expression replacement | 184–192 | 374–433 | – |
| Decode, then a replacement with `TextEncoder` for each escape | 195–202 | 1,268–1,308 | – |
| Decode, then a scan | 184–203 | 281–297 | – |
| Encoder A: a view for each call, a replacement | 190–200 | 520–529 | 168–169 |
| Encoder B: a view only for a tail, a replacement | 139–141 | 433–441 | 154–156 |
| Encoder C: a view only for a tail, a scan always | 207–212 | 305–311 | 153–162 |
| **Encoder of this RFC**: a view only for a tail, a test, a scan on a match | 145 | 339–358 | 155–167 |

The results set these rules for the encoder:

- The encoder makes a `subarray` view only for a tail. A view for each call costs about 50 ns (encoder A against
  encoder B).
- The encoder tests before it escapes. For text with no escapes, the regular-expression test costs about 1 ns
  after the decode, and a full scan costs about 60 ns (encoder C).
- The encoder escapes with a scan, not with a regular-expression replacement.
  The replacement calls a function for each match, and costs about 100 ns more than the scan for text with
  escapes (encoder B against the encoder of this RFC).
- The escape step must use the table of the 256 `%XX` strings.
  A `TextEncoder` call for each escaped character costs about 900 ns more than the table, for text with escapes.

The encoder of this RFC costs less than today's loop in each group: about 44% less for text with no escapes,
about 25% less for text with escapes, and about 58% less for binary keys.
The benchmark also checks that encoders A, B, and C give the same names as the encoder of this RFC,
and that each ASCII component keeps its name of today.

#### Code changes

- `packages/fokosdb/src/sharding/partition-id.ts`: replace `isSafeNameByte` and `encodeRangeComponent`.
  Keep `RANGE_MIN`, `RANGE_MAX`, and `rangePartitionDoName`. Write `~` after the shard group in
  `rangePartitionDoName` and `PartitionIdHelper.doName`. Update the comments that describe the encoding and the
  `.r.` and `.h.` markers.
- `packages/fokosdb/src/sharding/route-context.ts`: add the `~` check to `validateTopology`, and update the
  `doName` comment of `FokosRouteContext`.
- `packages/fokosdb/src/sharding/partition-id.test.ts`: add the tests below.
- `packages/fokosdb/src/shared/partition-context.ts`: set the partition shard group to `fokos.p.<tableName>`,
  and check the reserved prefix on the table name.
- `packages/fokosdb/src/client/db.ts`: build the coordinator shard group from the table name.
  Update the comment of the coordinator group.
- `packages/fokosdb/tsdown.config.ts`: raise `CLIENT_MAX_BYTES` from 90 KiB to 95 KiB.
  `partition-id.ts` is in the client bundle, which uses 89.9 KiB before this change,
  and the encoder adds code that the old percent-encoding loop did not have.
- `packages/fokosdb/src/shared/partition-context.ts`
- `packages/fokosdb/src/client/db.ts`
- `packages/fokosdb/test/do-names-bench/`: the benchmark of the performance section. It holds a copy of the
  encoder. The `bench:workerd` script runs it with `packages/fokosdb/test/vitest.workerd-bench.config.ts`.
  When the encoder moves into `partition-id.ts`, the benchmark must import it from there.

#### Testing

The tests must pin exact names, because each name is a permanent address.

- Each current test with a fixed name changes only by the `~` separator. The components of the ASCII keys in
  these tests stay the same.
- The two names of the section 1 example are different.
- `validateTopology` rejects a shard group with `~`, and accepts `fokos.tc.<name>` and `fokos.p.<name>`.
- For the table `orders`, the partition names start with `fokos.p.orders~`, and the coordinator names start with
  `fokos.tc.orders~`.
- A table name that starts with `fokos.` is still rejected.
- A step 2 test for a lone `0xF8` byte at the end, which must give the `~b` form.
- A key that ends with a 4-byte character, which must give its text with an empty tail.
- Fixed names for German, Japanese, and emoji keys.
- A boundary cut after each byte of a 2-byte, 3-byte, and 4-byte character.
- Each range of the escape set, at its first and last code point.
- The regular expression and the code unit check of the scan hold the same escape set in two forms.
  A test must check each code unit from 0 to 0xFFFF, and require that both forms give the same result.
  A difference between the two forms changes names, so this test must stay.
- A binary key, and a binary key that holds only ASCII bytes after its tag.
- A leading U+FEFF, which must stay in the name as `%EF%BB%BF`.
- Bytes that are not valid UTF-8 before the tail, which must give the `~b` form.
- A string key that equals `~min`, `~max`, or starts with `~b`, which must not collide with the reserved forms.
- A property test (`test/property-based/partition-names.test.ts`): a test-only parser must rebuild the original bytes
  from each name component, and the shard group and the identity from each DO name.

## 5. Alternative options

- **Percent-encoding for all components (today).** It is short and readable for ASCII keys.
  It uses three characters for each non-ASCII byte, and binary and non-English names are not readable.
- **base64url for all components.** Its length is always 4/3 of the bytes, and `toBase64` is native.
  No name is readable, and ASCII names become 33% longer than today. In section 8 the total is 1,019 characters
  for the keys, against 861 for percent-encoding.
- **A hybrid that counts unsafe bytes.** It percent-encodes a component while the percent form is not longer than
  the `~b` base64url form, and it uses base64url when the count of unsafe bytes exceeds the limit.
  It bounds the length, but non-English keys become base64url and are not readable.
  The choice also depends on the ratio of unsafe bytes, so a key and its boundary can get different forms.
- **Shared prefix for the end boundary.** The end boundary stores the length of the prefix it shares with the
  start boundary, and then its own suffix. It saves length on long keys, but the end boundary is not readable
  alone, and it needs one more fixed threshold in the name rules.
- **Truncated components plus a hash.** The name keeps the first characters of each component and adds a hash of
  the full identity. It bounds the name length. The hash must resist collisions, because a collision puts two
  partitions in one Durable Object. `crypto.subtle.digest` is asynchronous, and target construction is
  synchronous. A synchronous SHA-256 adds bundle size. Names have no length limit, so the gain is small.
- **Only a fixed shard group prefix, such as `fokos.p.`.** The collision is in the part after the prefix,
  so the prefix does not prevent it (section 1).
- **Reject `.` in the shard group.** The internal prefixes `fokos.tc.` and `fokos.p.` contain `.`, and DynamoDB
  table names can contain `.`.
- **Escape the shard group with the component encoder.** It makes the names unambiguous and permits any shard group.
  The internal prefixes become `fokos%2Etc%2E` and `fokos%2Ep%2E`, which are less readable, and each name needs one
  more encoding.
- **The partition ID in the name.** The name becomes `<shardGroup>~r.<partitionId>`. This ties the Durable Object
  to the ID format, so the ID format could not change later.
- **No end boundary in the name.** `(hashKey, start)` does not identify a range. A leftmost child has the start
  of its parent, and a split that runs again with other boundaries reuses a name of an earlier attempt.

## 6. Frequently asked questions

### Why is the name not always base64url, if base64url is shorter for non-ASCII bytes?

For non-English text, the decoded text is shorter than base64url and readable.
In section 8, the Japanese key uses 18 characters as text and 35 as base64url.
For ASCII keys, the text form is the percent form of today, and it is shorter than base64url.

### Does this change the name of a partition with an ASCII key?

Yes, but only by the `~` separator. The ASCII part of the escape set is the current unsafe byte set,
so an ASCII key gives the same components before and after the change.

### Why `~` as the separator?

The names already reserve `~`: the component encoding escapes it, and it marks `~min`, `~max`, and `~b`.
A DynamoDB table name cannot contain it, so the check in `validateTopology` does not reject a DynamoDB-style name.

### Why is a boundary with an incomplete UTF-8 character not base64url?

The incomplete sequence is only at the end, and it has at most 3 bytes.
The encoder keeps the complete characters as text and escapes only those bytes, so the boundary stays readable.

### Does production accept a non-ASCII Durable Object name?

Yes. Production `getByName` accepts non-ASCII names.
`ctx.id.name` returns a decoded form of the name, which can differ from the input string.
The code uses `ctx.id.name` only in logs (`actorName` in `do-partition.ts` and `runtime.ts`) and in the `doName`
attribute of a not-initialized error when the request has no route reference. Thus a difference has no effect
on routing.

### Why does the encoder not escape confusable characters?

A list of confusable characters comes from Unicode data, and that data changes between Unicode versions.
The name rules must not change, so they cannot depend on it.

## 7. References

- `packages/fokosdb/src/sharding/partition-id.ts`
- `packages/fokosdb/src/sharding/key-codec.ts`
- `packages/fokosdb/src/sharding/route-context.ts`
- `packages/fokosdb/src/shared/partition/partition-store.ts`
- `packages/fokosdb/src/shared/do-stubs.ts`
- `packages/fokosdb/src/server/do-partition.ts`
- `packages/fokosdb/src/sharding/partition-id.test.ts`
- `packages/fokosdb/src/shared/partition-context.ts`
- `packages/fokosdb/src/client/db.ts`
- `packages/fokosdb/test/do-names-bench/range-do-name.workerd-bench.ts`
- `packages/fokosdb/test/vitest.workerd-bench.config.ts`
- `docs/agent-plans/2026-09-26-range-partition-id-base64url.md`
- [DynamoDB naming rules](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html)

## 8. Appendix: encoded lengths for 20 common key formats

Each row is one key format. The "boundary" is `KeyCodec.shortestSeparator` of the key and a close lower key.
The lengths are in JavaScript string characters. "pct" is today's percent-encoding.
"b64" is unpadded base64url without the `~b` marker. "text" is the encoder of this RFC, with the marker.

| # | Kind | Key: pct | Key: b64 | Key: text | Boundary: pct | Boundary: b64 | Boundary: text | Boundary as text |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | ULID | 26 | 35 | 26 | 10 | 14 | 10 | `01HRZ8J6X7` |
| 2 | UUIDv4 text | 36 | 48 | 36 | 5 | 7 | 5 | `f47ac` |
| 3 | UUID binary (16 B) | 39 | 23 | 25 | 10 | 6 | 8 | `~b__R6wQ` |
| 4 | KSUID | 27 | 36 | 27 | 8 | 11 | 8 | `2ZgXkLmN` |
| 5 | Snowflake ID | 19 | 26 | 19 | 12 | 16 | 12 | `176754239759` |
| 6 | ISO-8601 ms timestamp | 26 | 32 | 26 | 15 | 20 | 15 | `2024-03-15T14:3` |
| 7 | DynamoDB `ORDER#ts#ULID` | 53 | 71 | 53 | 21 | 28 | 21 | `ORDER#2024-03-15T14:3` |
| 8 | DynamoDB `USER#email` | 29 | 34 | 29 | 7 | 10 | 7 | `USER#jo` |
| 9 | DynamoDB hierarchy | 25 | 34 | 25 | 13 | 18 | 13 | `USA#WA#King#S` |
| 10 | DynamoDB version prefix | 26 | 35 | 26 | 20 | 27 | 20 | `v0#INVOICE#2024-0315` |
| 11 | Email | 35 | 42 | 35 | 11 | 12 | 11 | `john%2Edoe+` |
| 12 | Git SHA-1 | 40 | 54 | 40 | 7 | 10 | 7 | `9fceb02` |
| 13 | S3 photo key | 43 | 55 | 43 | 34 | 46 | 34 | `photos/2024/03/15/IMG_20240315_143` |
| 14 | S3 Hive parquet | 110 | 142 | 110 | 38 | 51 | 38 | `logs/year=2024/month=03/day=15/hour=14` |
| 15 | S3 CloudTrail key | 130 | 168 | 130 | 99 | 132 | 99 | `AWSLogs/123456789012/CloudTrail/us-east-1/2024/03/15/123456789012_CloudTrail_us-east-1_20240315T143` |
| 16 | File name with spaces | 46 | 48 | 46 | 13 | 12 | 13 | `Q1%202024%20F` |
| 17 | File name, German | 43 | 39 | 33 | 21 | 20 | 16 | `Präsentation%20M` |
| 18 | File name, Japanese | 58 | 35 | 18 | 42 | 24 | 10 | `議事録_2024年3` |
| 19 | npm `package@version` | 18 | 19 | 18 | 18 | 19 | 18 | `lodash@4%2E17%2E21` |
| 20 | URL path | 32 | 43 | 32 | 31 | 42 | 31 | `/api/v2/users/12345/orders/9876` |
| | **Total** | 861 | 1,019 | 797 | 435 | 525 | 396 | |

The full keys of rows 3, 17, and 18 encode as follows. All other full keys encode as they do today.

```text
 3  ~b__R6wQtYzENypWcOArLD1Hk
17  Präsentation%20März%202024%2Epptx
18  議事録_2024年3月%2Edocx
```

Rows 17 and 18 hold multi-byte characters, so they use more UTF-8 bytes than characters.
The row 18 key uses 18 characters and 28 UTF-8 bytes.

Some edge cases:

```text
"議事録" cut after 7 bytes (inside 録)   -->  議事%E9
U+FEFF then "a"                         -->  %EF%BB%BFa
bytes 61 80 62 (invalid UTF-8)          -->  ~bYYBi
"a", U+200B, "b"                        -->  a%E2%80%8Bb
"🎉" cut after 2 bytes                   -->  %F0%9F
```
