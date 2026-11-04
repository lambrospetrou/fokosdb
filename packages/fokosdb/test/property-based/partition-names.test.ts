// Property-based tests for the partition ID wire formats and the Durable Object names.
//
// A DO name is the address of the partition data: `getByName` hashes it. Two partitions that get one
// name share one Durable Object and its data, and a name that changes makes the data unreachable.
// The properties check both directions with an oracle of this file:
//
//   - Each partition ID decodes back to the identity that built it.
//   - Each DO name parses back to exactly the shard group and the identity that built it. A name that
//     parses back to its input cannot also be the name of a different input, so this proves that the
//     names are injective across shard groups, across hash and range partitions, and across keys.
//   - A component parses back to its bytes, also for bytes that no key gives.
//   - An ASCII component keeps the percent-encoding that the names used before the text form.
//
// The oracle never calls the encoder. It parses the name form: `<shardGroup>~h.<root>[.<child>...]`
// and `<shardGroup>~r.<routingHash hex>.<start>.<end>`, where an edge is `~min`, `~max`, or the two
// components of a hash key and a sort key, and a component is `~b` and base64url, or text with `%XX`
// escapes. The escape set of the oracle is its own copy, so a change to the escape
// set of the encoder fails here.
//
// No property uses a Durable Object, so a run costs microseconds and the default run count is high.
// harness.ts says how to replay a failure.
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { KeyCodec, type FokosCompositeKey, type KeyBytes } from "../../src/sharding/key-codec.js";
import { encodeRangeComponent, isHashPartition, isRangePartition, PartitionIdHelper } from "../../src/sharding/partition-id.js";
import { asRoutingHash, type RoutingHash } from "../../src/sharding/hash-primitives.js";
import { propertyRuns, textEncoder } from "./harness.js";

const PROPERTY_RUNS = propertyRuns(2_000);
// A run has no I/O and takes less than a millisecond. The floor keeps the default limit of a test
// for a low run count.
const PROPERTY_TIMEOUT_MS = Math.max(45_000, PROPERTY_RUNS * 20);

// ─── The oracle ───────────────────────────────────────────────────────────────

/** The escape set of the text form, as inclusive code point ranges. */
const ESCAPE_RANGES: [number, number][] = [
	[0x0000, 0x0020],
	[0x0022, 0x0022],
	[0x0025, 0x0025],
	[0x002e, 0x002e],
	[0x005c, 0x005c],
	[0x007e, 0x009f],
	[0x200b, 0x200f],
	[0x2028, 0x202e],
	[0x2066, 0x2069],
	[0xfeff, 0xfeff],
];
const ESCAPE_CHARS = ESCAPE_RANGES.flatMap(([first, last]) =>
	Array.from({ length: last - first + 1 }, (_, i) => String.fromCodePoint(first + i)),
);
const ESCAPE_SET = new Set(ESCAPE_CHARS);

/** The bytes of one name component, or null for `~min` and `~max`. It throws on a malformed component. */
function parseComponent(component: string): Uint8Array | null {
	if (component === "~min" || component === "~max") {
		return null;
	}
	if (component.startsWith("~b")) {
		const text = component.slice(2);
		expect(text).toMatch(/^[A-Za-z0-9_-]+$/);
		// Strict mode rejects non-zero trailing bits, so each byte string has exactly one text.
		const padded = text.padEnd(Math.ceil(text.length / 4) * 4, "=");
		const bytes = Uint8Array.fromBase64(padded, { alphabet: "base64url", lastChunkHandling: "strict" });
		// The ~b form holds a binary key, or bytes that are not UTF-8 before an incomplete tail.
		expect(bytes.length).toBeGreaterThan(0);
		return bytes;
	}
	const out: number[] = [];
	// "%" is ASCII, so a text part never splits a surrogate pair. "%" is in the escape set, so each
	// "%" of the text form starts an escape, and the parts cover the whole component.
	const parts = component.match(/%[0-9A-F]{2}|[^%]+/g) ?? [];
	expect(parts.join("")).toBe(component);
	for (const part of parts) {
		if (part.startsWith("%")) {
			out.push(Number.parseInt(part.slice(1), 16));
			continue;
		}
		for (const char of part) {
			expect(ESCAPE_SET.has(char), `the text form holds an unescaped ${JSON.stringify(char)}`).toBe(false);
		}
		out.push(...textEncoder.encode(part));
	}
	return new Uint8Array(out);
}

type ParsedName =
	| { shardGroup: string; kind: "h"; root: number; path: number[] }
	| { shardGroup: string; kind: "r"; routingHash: Uint8Array; start: ParsedBoundary | null; end: ParsedBoundary | null };

type ParsedBoundary = { hk: Uint8Array; sk: Uint8Array };

/** The shard group and the identity of a DO name. */
function parseDoName(name: string): ParsedName {
	const separator = name.indexOf("~");
	expect(separator).toBeGreaterThan(0);
	const shardGroup = name.slice(0, separator);
	const rest = name.slice(separator + 1);
	if (rest.startsWith("h.")) {
		const indexes = rest.slice(2).split(".");
		for (const index of indexes) {
			expect(index).toMatch(/^(0|[1-9][0-9]*)$/);
		}
		const [root, ...path] = indexes.map(Number);
		return { shardGroup, kind: "h", root, path };
	}
	expect(rest.startsWith("r.")).toBe(true);
	const [routingHashHex, ...components] = rest.slice(2).split(".");
	expect(routingHashHex).toMatch(/^[0-9a-f]{16}$/);
	// "~min" is only a start, and "~max" is only an end. A bounded edge has two components.
	const edge = (unbounded: string): ParsedBoundary | null => {
		if (components[0] === unbounded) {
			components.shift();
			return null;
		}
		const [hk, sk] = components.splice(0, 2);
		expect(sk).toBeDefined();
		const parsed = { hk: parseComponent(hk), sk: parseComponent(sk) };
		expect(parsed.hk).not.toBeNull();
		expect(parsed.sk).not.toBeNull();
		return { hk: parsed.hk!, sk: parsed.sk! };
	};
	const start = edge("~min");
	const end = edge("~max");
	expect(components).toHaveLength(0);
	return { shardGroup, kind: "r", routingHash: Uint8Array.fromHex(routingHashHex), start, end };
}

/** The percent-encoding of each byte: printable ASCII except `"`, `%`, `.`, `\` and `~` stays literal. */
function asciiPercentEncoding(bytes: Uint8Array): string {
	let out = "";
	for (const b of bytes) {
		const literal = b >= 0x21 && b <= 0x7d && b !== 0x22 && b !== 0x25 && b !== 0x2e && b !== 0x5c;
		out += literal ? String.fromCharCode(b) : "%" + b.toString(16).padStart(2, "0").toUpperCase();
	}
	return out;
}

// ─── The arbitraries ──────────────────────────────────────────────────────────

/** Text with many characters that the escape set holds, and characters of 1 to 4 UTF-8 bytes. */
const arbText = fc.string({
	unit: fc.oneof(
		{ arbitrary: fc.constantFrom(...ESCAPE_CHARS), weight: 2 },
		{
			arbitrary: fc.constantFrom("a", "Z", "0", "#", "/", "-", "_", "ä", "ß", "録", "年", "\u{1F389}", "\u{10FFFF}", "~min", "~b"),
			weight: 4,
		},
		{ arbitrary: fc.string({ unit: "binary", minLength: 1, maxLength: 1 }), weight: 2 },
	),
	minLength: 1,
	maxLength: 100,
});

/** The canonical bytes of a string key or a binary key. */
const arbKey: fc.Arbitrary<KeyBytes> = fc.oneof(
	arbText.map((text) => KeyCodec.encode(text)),
	fc.uint8Array({ minLength: 1, maxLength: 100 }).map((bytes) => KeyCodec.encode(bytes)),
);

/**
 * A range boundary: the shortest separator of two keys. A separator of two string keys can end inside
 * a multi-byte character, which gives the text form an incomplete tail.
 */
const arbBoundary: fc.Arbitrary<KeyBytes> = fc
	.tuple(arbKey, arbKey)
	.filter(([a, b]) => KeyCodec.compare(a, b) !== 0)
	.map(([a, b]) => (KeyCodec.compare(a, b) < 0 ? KeyCodec.shortestSeparator(a, b) : KeyCodec.shortestSeparator(b, a)));

/** Any component the encoder can get: a key, a boundary, or bytes that no key gives. */
const arbComponent: fc.Arbitrary<KeyBytes> = fc.oneof(
	arbKey,
	arbBoundary,
	fc.uint8Array({ maxLength: 16 }).map((bytes) => KeyCodec.asKeyBytes(bytes)),
);

/**
 * A composite range boundary: the shortest separator of two composite keys, or a hash key with any
 * sort key component, also the empty sort key.
 */
const arbCompositeBoundary: fc.Arbitrary<FokosCompositeKey> = fc.oneof(
	fc
		.tuple(arbKey, arbKey, arbKey, arbKey)
		.map(([ahk, ask, bhk, bsk]) => [
			{ hk: ahk, sk: ask },
			{ hk: bhk, sk: bsk },
		])
		.filter(([a, b]) => KeyCodec.compareComposite(a, b) !== 0)
		.map(([a, b]) =>
			KeyCodec.compareComposite(a, b) < 0 ? KeyCodec.shortestCompositeSeparator(a, b) : KeyCodec.shortestCompositeSeparator(b, a),
		),
	fc.tuple(arbKey, arbComponent).map(([hk, sk]) => ({ hk, sk })),
);

const arbOptionalBoundary = fc.option(arbCompositeBoundary, { nil: null });

/** A shard group: any text without "~", with dots, and with the prefixes of FokosDB. */
const arbShardGroup = fc
	.tuple(
		fc.constantFrom("", "fokos.p.", "fokos.tc."),
		fc.string({ unit: fc.constantFrom("x", "r", "h", "k", "0", ".", "-", "_", "é"), minLength: 1, maxLength: 8 }),
	)
	.map(([prefix, name]) => prefix + name);

const arbHashIdxs = fc.tuple(fc.integer({ min: 0, max: 0xffff }), fc.array(fc.integer({ min: 0, max: 255 }), { maxLength: 6 }));

type Partition =
	| { shardGroup: string; kind: "h"; root: number; path: number[] }
	| { shardGroup: string; kind: "r"; routingHash: RoutingHash; start: FokosCompositeKey | null; end: FokosCompositeKey | null };

const arbPartition: fc.Arbitrary<Partition> = fc.oneof(
	fc
		.record({ shardGroup: arbShardGroup, kind: fc.constant("h" as const), idxs: arbHashIdxs })
		.map(({ shardGroup, idxs: [root, path] }) => ({
			shardGroup,
			kind: "h" as const,
			root,
			path,
		})),
	fc.record({
		shardGroup: arbShardGroup,
		kind: fc.constant("r" as const),
		routingHash: fc.uint8Array({ minLength: 8, maxLength: 8 }).map(asRoutingHash),
		start: arbOptionalBoundary,
		end: arbOptionalBoundary,
	}),
);

function encodePartition(partition: Partition): { bytes: Uint8Array; opaque: string; doName: string } {
	const helper =
		partition.kind === "h"
			? PartitionIdHelper.fromHashIdxs(partition.shardGroup, [partition.root, ...partition.path])
			: PartitionIdHelper.fromRangePartition(partition.shardGroup, partition.routingHash, partition.start, partition.end);
	const { bytes, opaque, doName } = helper.encode(true);
	return { bytes, opaque, doName: doName! };
}

/** A partition as the oracle parses it back from its DO name. */
function expectedParse(partition: Partition): ParsedName {
	if (partition.kind === "h") {
		return partition;
	}
	const copy = (key: FokosCompositeKey | null) => (key === null ? null : { hk: new Uint8Array(key.hk), sk: new Uint8Array(key.sk) });
	return {
		shardGroup: partition.shardGroup,
		kind: "r",
		routingHash: new Uint8Array(partition.routingHash),
		start: copy(partition.start),
		end: copy(partition.end),
	};
}

// ─── The properties ───────────────────────────────────────────────────────────

describe("partition IDs and DO names — properties", () => {
	it("a partition ID decodes back to the identity that built it", { timeout: PROPERTY_TIMEOUT_MS }, () => {
		fc.assert(
			fc.property(arbPartition, (partition) => {
				const { bytes, opaque } = encodePartition(partition);
				expect(PartitionIdHelper.partitionIdToBytes(opaque)).toEqual(bytes);
				expect(isHashPartition({ partitionId: opaque })).toBe(partition.kind === "h");
				expect(isRangePartition({ partitionId: opaque })).toBe(partition.kind === "r");
				const decoded = PartitionIdHelper.decode(PartitionIdHelper.partitionIdToBytes(opaque));
				if (partition.kind === "h") {
					expect(decoded).toEqual({ schema: 0, rootIdx: partition.root, depth: partition.path.length });
					expect(Array.from(bytes.subarray(4))).toEqual(partition.path);
				} else {
					expect(decoded).toEqual({
						schema: 1,
						routingHash: partition.routingHash,
						startBoundary: partition.start,
						endBoundary: partition.end,
					});
					expect(PartitionIdHelper.rangeRoutingHash(opaque)).toEqual(partition.routingHash);
				}
				// The DO name of the decoded ID is the DO name of the partition.
				expect(new PartitionIdHelper(partition.shardGroup, opaque).encode(true).doName).toBe(encodePartition(partition).doName);
			}),
			{ numRuns: PROPERTY_RUNS },
		);
	});

	it("a DO name parses back to the shard group and the identity that built it", { timeout: PROPERTY_TIMEOUT_MS }, () => {
		fc.assert(
			fc.property(arbPartition, (partition) => {
				expect(parseDoName(encodePartition(partition).doName)).toEqual(expectedParse(partition));
			}),
			{ numRuns: PROPERTY_RUNS },
		);
	});

	it("a component parses back to its bytes, and never holds '.' or starts with '~m'", { timeout: PROPERTY_TIMEOUT_MS }, () => {
		fc.assert(
			fc.property(arbComponent, (bytes) => {
				const component = encodeRangeComponent(bytes);
				expect(component).not.toContain(".");
				expect(component.startsWith("~m")).toBe(false);
				expect(parseComponent(component)).toEqual(new Uint8Array(bytes));
				// The encoding is deterministic, also for a copy of the bytes in another buffer.
				expect(encodeRangeComponent(KeyCodec.asKeyBytes(new Uint8Array(bytes)))).toBe(component);
			}),
			{ numRuns: PROPERTY_RUNS },
		);
	});

	it("a component of a string key is text, and an ASCII component keeps its percent-encoding", { timeout: PROPERTY_TIMEOUT_MS }, () => {
		fc.assert(
			fc.property(
				fc.oneof(
					arbText.map((text) => KeyCodec.encode(text)),
					arbBoundary,
				),
				(bytes) => {
					const component = encodeRangeComponent(bytes);
					if (bytes[0] !== 0xff) {
						// A string key and each boundary of string keys are valid UTF-8 before a tail.
						expect(component.startsWith("~b")).toBe(false);
					}
					if (bytes.every((b) => b < 0x80)) {
						expect(component).toBe(asciiPercentEncoding(bytes));
					}
				},
			),
			{ numRuns: PROPERTY_RUNS },
		);
	});
});
