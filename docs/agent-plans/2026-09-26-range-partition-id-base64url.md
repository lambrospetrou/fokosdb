# RFC — Compact range partition IDs with two base64url parts

**State:** Draft  
**Date:** 2026-09-26  
**Author:** Lambros Petrou

**Status:** No code change exists yet. The two-part ID and direct DO-name construction are the core change.
Boundary-prefix shortening is an extension, not part of the core change.

## Table of contents

- [1. Overview and context](#1-overview-and-context)
- [2. Goals and requirements](#2-goals-and-requirements)
- [3. Milestones](#3-milestones)
- [4. Proposed solution](#4-proposed-solution)
  - [4.1 High-level overview](#41-high-level-overview)
  - [4.2 Technical details](#42-technical-details)
  - [4.3 Extension: boundary-prefix shortening](#43-extension-boundary-prefix-shortening)
  - [4.4 Open questions](#44-open-questions)
- [5. Alternative options](#5-alternative-options)
- [6. Frequently asked questions](#6-frequently-asked-questions)
- [7. References](#7-references)

## 1. Overview and context

`PartitionIdHelper` stores a range ID as hex text over a header, a hash key, and two range boundaries.
The encoded bytes use `10 + H + S + E` bytes, where `H`, `S`, and `E` are canonical key lengths.
The ID string uses twice that number of characters.
FokosDB permits a 1,024-byte hash key and 512-byte sort keys after key encoding.
At those lengths, a range ID with two boundaries uses 4,116 hex characters.

A forwarding partition learns from each forwarded result or routed error.
Today, it decodes the full ID of each route node, but uses only the range hash key.
It learns range boundaries from the separate `_rangeAncestors` response field.
The runtime can repeat this work at each forwarding hop.

A partition also creates an ID and a DO name when it routes to a range root or learned range slice.
Today, the encoder decodes its new ID to recover the inputs for the DO name.
This can occur on a request that uses a learned range route.
The codec can create the name directly from the original canonical keys instead.

`docs/ideas/range-partition-id-hierarchy-encoding.md` keeps ancestry in response hints, outside the ID.
This plan keeps that separation but changes the ID string format, which the earlier idea left unchanged.

## 2. Goals and requirements

### 2.1 In scope

- The range ID must use two unpadded base64url parts with one dot between them.
- The first part must contain the hash key and the header needed to read it.
- The second part must contain the range boundaries.
- The range ID must retain the `"01"` kind prefix. Hash IDs must retain their current hex format.
- A forwarding partition must learn a range hash key without decoding the boundary part.
- A new range partition must decode both parts to establish its identity.
- The normal range-context constructor must create the ID and DO name from the same canonical inputs.
- The DO name must retain its current format, escaping rules, and unbounded-edge tokens.
- The codec must distinguish a null boundary from a present boundary by its flags.
- The codec must create one canonical ID string for each range identity.

### 2.2 Out of scope

- The hash ID layout and encoding remain unchanged because hash IDs are not the storage problem here.
- The `0x01` range schema value and the two four-byte length fields remain unchanged.
- The extension in section 4.3 needs a separate decision before it changes the core format.
- Compatibility with existing persisted range IDs is out of scope. This change breaks their string format.
- Changes to ancestor selection or response hints are out of scope.
  `docs/ideas/2026-09-26-range-self-hint-bugfix.md` tracks the missing self hint.
- The range DO name does not change. The ID and the DO name serve different purposes.

### 2.3 Requirements

- Both parts must use canonical, unpadded base64url text. The ID must contain exactly one separator dot.
- A malformed part, invalid flag set, or inconsistent length must fail during full ID decoding.
- Range keys and boundaries must keep their canonical `KeyBytes` without text conversion.
- A forwarding response must not require the second part to learn its hash key or its ancestor hints.
- A repeated request to an initialized partition must compare identity strings, not decode its ID.
- The codec must keep full ID decoding available when a caller has no original keys.
- The change must preserve deterministic ID and DO-name construction for every supported key type.

## 3. Milestones

1. Change the range codec and context construction. Keep the hash codec and DO names unchanged.
2. Change the runtime's response-learning path to decode the first range part only.
3. Update the codec tests, test helpers, and range-routing suites. Measure the request and response paths.

The extension in section 4.3 is separate from these milestones.

## 4. Proposed solution

### 4.1 High-level overview

The range ID starts with `"01"`. Its first base64url part contains the header and hash key.
A dot separates that part from a second base64url part that contains the boundaries.
The range DO name still uses the hash key and both boundaries in its existing readable format.

```text
canonical hash key, start, end
  |-- encode header + hash key --> "01" + base64url(first part)
  |-- encode start + end       --> "." + base64url(second part)
  `-- encode DO name           --> <shardGroup>.r.<hk>.<start>.<end>

forwarded response --> decode first part only --> hash key for route learning
first fokosInit    --> decode both parts     --> stored partition identity
```

The constructor makes the ID and DO name from the same `KeyBytes` inputs.
It does not decode the ID that it just made.
A response learner does not read the boundary part.
After initialization, the partition uses its stored decoded identity for ownership checks.

### 4.2 Technical details

#### ID layout

The range ID is the following string. The dot is a separator, not base64url data.

```text
"01" + base64url(first) + "." + base64url(second)

first:
  flags:     u8       bit 0 = has start; bit 1 = has end
  hkLen:     u32 LE   number of canonical hash-key bytes
  startLen:  u32 LE   number of start bytes; 0 when start is null
  hashKey:   hkLen canonical KeyBytes

second:
  start:     startLen canonical KeyBytes, when the start flag is set
  end:       remaining canonical KeyBytes, when the end flag is set
```

The encoder writes an empty second part for a range root, so its ID ends with a dot.
It must not omit that dot or add padding. The flags distinguish null from present boundaries.
The decoder uses `hkLen` to check the first part and `startLen` to split the second part.
When the end flag is clear, the second part must have no bytes after the start.
When the start flag is clear, `startLen` must be zero.

`PartitionIdHelper.encode(...).bytes` must keep the flat byte layout with a leading `0x01` schema byte.
Only `opaque` changes to the two-part string. A full ID reader can rebuild the flat bytes from both parts.

The string prefix keeps the existing cheap `"00"` versus `"01"` kind check.
The hash codec keeps its current byte layout and hex string.
A helper that takes an opaque ID must select the correct decoder before it reads bytes.
This includes `PartitionIdHelper.partitionIdToBytes` and the helper constructor.

#### Request path and DO name

`resolveRangePartitionContext` receives the hash key and boundaries as canonical `KeyBytes`.
It must build both base64url parts from those inputs.
It must call the existing range-name encoder on the same inputs, without an ID decode.
The name must keep the current `%XX` escapes and `~min` and `~max` tokens.

`PartitionIdHelper.doName` can still derive a name when only decoded ID bytes exist.
That capability must not run in the normal range-context constructor.
A direct child already carries its ID and name in its stored reference.
A learned range target can require a new ID and name on a request.

An initialized partition compares the supplied ID and name with its stored identity.
Its ownership checks use the already decoded hash key and boundaries.
Neither operation decodes the ID on each request.

#### Response path and identity creation

`FokosShardingRuntime.#learn` must check the ID kind before any hex decode.
For a range node, it must decode only `first` and use that part's hash key.
It must continue to read learned boundaries from `_rangeAncestors`.
The same path applies to successful results and routed errors.
For a hash node, it must retain the current hash-path check.

On the first `fokosInit`, `partitionIdentityFrom` must decode both parts of a range ID.
It stores the hash key and both boundaries in `FokosPartitionIdentity`.
The runtime loads that decoded identity from storage after a restart.
A repeated `fokosInit` checks the stored identity without a full ID decode.

The query leaf metrics, route collector, and public metadata use ID string equality or copy the ID.
They must not decode either range part.
The `rangeOf` test helper must use the new full range decoder.

#### Performance, deployment, and verification

For 50-byte hash keys and two 100-byte boundaries, the ID changes from 520 to 349 characters.
For 1,024-byte hash keys and two 512-byte boundaries, it changes from 4,116 to 2,747 characters.
These lengths follow the key caps and the unpadded base64url layout; they are not latency results.
The second encoding saves bytes on the wire, but the DO name and `_rangeAncestors` stay unchanged.

The response learner decodes bytes that grow with `H`, not with `S + E`.
A learned target still encodes its ID and DO name from its routing keys on the request path.
TODO: Measure both paths in the Workers runtime against the current hex format.

This is a breaking ID change. This plan does not migrate identities that existing partitions store.
A deployment with existing partition data needs a separate migration plan before it uses this format.

The tests must cover null, one-sided, and two-sided bounds; string, Unicode, and binary keys;
reserved DO-name bytes; and exact byte-for-byte round trips.
They must test canonical base64url, malformed separators, invalid lengths, and flag/byte mismatches.
They must confirm that names built from inputs equal names built by full ID decoding.
Routing tests must cover learned range targets, forwarded results, routed errors, and hash IDs.

### 4.3 Extension: boundary-prefix shortening

The extension can store the common byte prefix of `start` and `end` once.
It applies only when both boundaries exist and share enough bytes to save space.
It keeps `first` unchanged so response learning still reads no boundary bytes.
This extension is not approved for the core change.

A reserved flag bit can mark the compact `second` part.
That part can store a two-byte shared-prefix length, the full start, and the end suffix.
The full decoder reconstructs the end from the start prefix and the end suffix.
The DO name still comes from the original full boundaries during target construction.

The extension adds a prefix scan when a request constructs a learned target.
It adds a boundary copy when a partition fully decodes its identity.
It does not add either operation to the response-learning path.
The encoder must choose one canonical form for a given range identity.

#### Workerd benchmark

A temporary Vitest prototype used `@cloudflare/vitest-plugin` to run target construction inside workerd.
It compared the two-part base64url format with and without the prefix extension.
The prototype built both an ID and a DO name, but ran no SQL, stub lookup, or RPC.
It used one 14-byte hash key and safe ASCII boundaries of the stated length.
Each boundary pair shared the stated fraction of its bytes; the remaining bytes were `a` and `b`.
The prototype used the compact form when the shared prefix exceeded two bytes.
This rule is for the measurement only. It does not decide the production threshold.

The times are median microseconds per target over five batches of repeated target constructions.
The ID lengths are ASCII characters, which have the same byte count in UTF-8.
The DO-name length is the same for both formats.

| Boundary each | Shared | ID chars: plain → prefix | DO-name chars | µs/target: plain → prefix | Time change |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 10 B | 10% | 61 → 61 | 44 | 0.867 → 0.867 | 0% |
| 10 B | 30% | 61 → 60 | 44 | 0.875 → 0.892 | +1.9% |
| 50 B | 10% | 168 → 164 | 124 | 1.571 → 1.586 | +0.9% |
| 50 B | 30% | 168 → 150 | 124 | 1.586 → 1.614 | +1.8% |
| 100 B | 10% | 301 → 290 | 224 | 2.200 → 2.220 | +0.9% |
| 100 B | 30% | 301 → 264 | 224 | 2.160 → 2.200 | +1.9% |
| 1,000 B | 10% | 2,701 → 2,570 | 2,024 | 12.625 → 12.500 | −1.0% |
| 1,000 B | 30% | 2,701 → 2,304 | 2,024 | 12.375 → 13.000 | +5.1% |
| 2,000 B | 10% | 5,368 → 5,104 | 4,024 | 23.500 → 23.750 | +1.1% |
| 2,000 B | 30% | 5,368 → 4,570 | 4,024 | 23.500 → 25.000 | +6.4% |
| 5,000 B | 10% | 13,368 → 12,704 | 10,024 | 58.500 → 61.000 | +4.3% |
| 5,000 B | 30% | 13,368 → 11,370 | 10,024 | 58.500 → 59.500 | +1.7% |

FokosDB caps each encoded sort key at 512 bytes.
The 1,000-, 2,000-, and 5,000-byte cases measure codec scaling, not valid public FokosDB keys.
The prototype is not the production codec. Its times do not measure the cost of a complete request.
The results show a size saving but do not establish that this extension is necessary.

### 4.4 Open questions

#### Boundary-prefix rule

What minimum shared-prefix length saves enough space to justify the request-side scan?
A measurement on real boundary distributions must decide whether to enable the extension.
It must also set the one canonical encoding rule for each range identity.

## 5. Alternative options

- One base64url part saves one or two more characters for the examples above.
  The learner would need a calculated base64url prefix or a full decode to avoid the boundary bytes.
- Shorter length fields save bytes in the header but do not address long boundary bytes.
- A DO name derived by decoding each new ID adds an avoidable parse on range-context construction.

## 6. Frequently asked questions

### Why does a range ID still contain its own boundaries?

A new partition gets its immutable range identity from the ID at initialization.
The response ancestor set is routing metadata, not the partition's identity.

### Does the response learner decode range boundaries?

No. It decodes the first part for the hash key and reads ancestor boundaries from the response.
The separate self-hint bug does not change this format.

### Does this change hash partitions or DO names?

No. Hash IDs keep their current encoding. Range DO names keep their current construction and escaping.

## 7. References

- `packages/fokosdb/src/sharding/partition-id.ts`
- `packages/fokosdb/src/sharding/route-context.ts`
- `packages/fokosdb/src/sharding/runtime.ts`
- `packages/fokosdb/src/sharding/envelope.ts`
- `packages/fokosdb/src/sharding/key-codec.ts`
- `packages/fokosdb/src/shared/transaction-limits.ts`
- `packages/fokosdb/test/partition-do/partition-harness.ts`
- `packages/fokosdb/vitest.config.ts`
- `docs/ideas/range-partition-id-hierarchy-encoding.md`
- `docs/ideas/2026-09-26-range-self-hint-bugfix.md`
