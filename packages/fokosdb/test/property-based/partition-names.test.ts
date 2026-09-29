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
// and `<shardGroup>~r.<hk>.<start>.<end>`, where a component is `~min`, `~max`, `~b` and base64url,
// or text with `%XX` escapes. The escape set of the oracle is its own copy, so a change to the escape
// set of the encoder fails here.
//
// No property uses a Durable Object, so a run costs microseconds and the default run count is high.
// harness.ts says how to replay a failure.
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { KeyCodec, type KeyBytes } from "../../src/sharding/key-codec.js";
import { encodeRangeComponent, isHashPartition, isRangePartition, PartitionIdHelper } from "../../src/sharding/partition-id.js";
import { propertyRuns, textEncoder } from "./harness.js";

const PROPERTY_RUNS = propertyRuns(2_000);

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
	| { shardGroup: string; kind: "r"; hashKey: Uint8Array; start: Uint8Array | null; end: Uint8Array | null };

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
	const components = rest.slice(2).split(".");
	expect(components).toHaveLength(3);
	const [hashKey, start, end] = components.map(parseComponent);
	expect(hashKey).not.toBeNull();
	// "~min" is only a start, and "~max" is only an end.
	expect(components[1]).not.toBe("~max");
	expect(components[2]).not.toBe("~min");
	return { shardGroup, kind: "r", hashKey: hashKey!, start, end };
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

const arbOptionalBoundary = fc.option(arbBoundary, { nil: null });

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
	| { shardGroup: string; kind: "r"; hashKey: KeyBytes; start: KeyBytes | null; end: KeyBytes | null };

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
		hashKey: arbKey,
		start: arbOptionalBoundary,
		end: arbOptionalBoundary,
	}),
);

function encodePartition(partition: Partition): { bytes: Uint8Array; opaque: string; doName: string } {
	const helper =
		partition.kind === "h"
			? PartitionIdHelper.fromHashIdxs(partition.shardGroup, [partition.root, ...partition.path])
			: PartitionIdHelper.fromRangePartition(partition.shardGroup, partition.hashKey, partition.start, partition.end);
	const { bytes, opaque, doName } = helper.encode(true);
	return { bytes, opaque, doName: doName! };
}

/** A partition as the oracle parses it back from its DO name. */
function expectedParse(partition: Partition): ParsedName {
	if (partition.kind === "h") {
		return partition;
	}
	const copy = (bytes: KeyBytes | null) => (bytes === null ? null : new Uint8Array(bytes));
	return {
		shardGroup: partition.shardGroup,
		kind: "r",
		hashKey: copy(partition.hashKey)!,
		start: copy(partition.start),
		end: copy(partition.end),
	};
}

// ─── The properties ───────────────────────────────────────────────────────────

describe("partition IDs and DO names — properties", () => {
	it("a partition ID decodes back to the identity that built it", () => {
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
					expect(decoded).toEqual({ schema: 1, hashKey: partition.hashKey, startBoundary: partition.start, endBoundary: partition.end });
					expect(PartitionIdHelper.rangeHashKey(opaque)).toEqual(partition.hashKey);
				}
				// The DO name of the decoded ID is the DO name of the partition.
				expect(new PartitionIdHelper(partition.shardGroup, opaque).encode(true).doName).toBe(encodePartition(partition).doName);
			}),
			{ numRuns: PROPERTY_RUNS },
		);
	});

	it("a DO name parses back to the shard group and the identity that built it", () => {
		fc.assert(
			fc.property(arbPartition, (partition) => {
				expect(parseDoName(encodePartition(partition).doName)).toEqual(expectedParse(partition));
			}),
			{ numRuns: PROPERTY_RUNS },
		);
	});

	it("a component parses back to its bytes, and never holds '.' or starts with '~m'", () => {
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

	it("a component of a string key is text, and an ASCII component keeps its percent-encoding", () => {
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
