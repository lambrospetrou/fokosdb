import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { propertyRuns } from "../../../test/property-based/harness.js";
import { KeyCodec, type KeyBytes } from "../../sharding/key-codec.js";
import { decodeGsiSortKey, encodeGsiSortKey, escapeKey, gsiEntryPrefixLength, gsiItemSortKey } from "./sort-key.js";

const k = (key: string | Uint8Array) => KeyCodec.encode(key);
const NONE = KeyCodec.encodeOptional(undefined);
const ascii = (bytes: Uint8Array) => new TextDecoder().decode(bytes);

describe("encodeGsiSortKey", () => {
	it("writes the escaped components, the separators, and the <L>:<D> trailer", () => {
		const physical = encodeGsiSortKey(k("2026-10-17"), k("u1"), k("o9"));
		expect(ascii(physical)).toBe("2026-10-17\0\0u1\0\0o9\0\x0010:2");
	});

	it("escapes each 0x00 byte and appends the raw GSI sort key with the '!' flag", () => {
		const gsiSK = k(Uint8Array.of(0x61, 0x00, 0x62));
		const physical = encodeGsiSortKey(gsiSK, k("u1"), k("o9"));
		expect([...physical]).toEqual([
			0xff, 0x61, 0x00, 0xff, 0x62, 0, 0, 0x75, 0x31, 0, 0, 0x6f, 0x39, 0, 0, 0xff, 0x61, 0x00, 0x62, 0x34, 0x21, 0x31,
		]);
	});

	it("writes 0:1 when the GSI has no sort key", () => {
		expect(ascii(encodeGsiSortKey(NONE, k("u1"), NONE))).toBe("\0\0u1\0\0\0\x000:1");
	});

	it("decodes each component back, for text, binary, and absent keys", () => {
		const cases: Array<[KeyBytes, KeyBytes, KeyBytes]> = [
			[k("a"), k("b"), k("c")],
			[NONE, k("h"), NONE],
			[k(Uint8Array.of(0, 0, 0)), k(Uint8Array.of(0)), k("x\u0000y")],
			[k("x".repeat(1234)), k(Uint8Array.of(0xff, 0, 0xff)), k("s")],
		];
		for (const [gsiSK, baseHK, baseSK] of cases) {
			expect(decodeGsiSortKey(encodeGsiSortKey(gsiSK, baseHK, baseSK))).toEqual({ gsiSK, baseHK, baseSK });
		}
	});

	it("sorts by GSI sort key, then by base hash key, then by base sort key", () => {
		const keys: Array<[string | Uint8Array | undefined, string, string]> = [
			[undefined, "b", "s"],
			[undefined, "c", "a"],
			["a", "a", "z"],
			["a", "b", "a"],
			["a\u0000", "a", "a"],
			["ab", "a", "a"],
			["b", "a", "a"],
			[Uint8Array.of(0), "a", "a"],
			[Uint8Array.of(0, 1), "a", "a"],
			[Uint8Array.of(1), "a", "a"],
		];
		const encoded = keys.map(([g, h, s]) => KeyCodec.asKeyBytes(encodeGsiSortKey(KeyCodec.encodeOptional(g), k(h), k(s))));
		for (let i = 1; i < encoded.length; i++) {
			expect(KeyCodec.compare(encoded[i - 1], encoded[i]), `entry ${i}`).toBeLessThan(0);
		}
	});

	it("puts every entry of one GSI sort key inside [E(k), succ(E(k)))", () => {
		const prefix = Uint8Array.of(0x61, 0, 0);
		const next = Uint8Array.of(0x61, 0, 1);
		for (const baseHK of ["a", "\u0000", "zzzz"]) {
			const physical = KeyCodec.asKeyBytes(encodeGsiSortKey(k("a"), k(baseHK), k(Uint8Array.of(0))));
			expect(KeyCodec.compare(KeyCodec.asKeyBytes(prefix), physical)).toBeLessThan(0);
			expect(KeyCodec.compare(physical, KeyCodec.asKeyBytes(next))).toBeLessThan(0);
		}
	});

	it("sorts a value before each longer value that starts with it, in each component", () => {
		// The values in the order of KeyCodec.compare: each string sorts before each binary value.
		const values: Array<string | Uint8Array> = [
			"order",
			"order#01",
			"order#0100",
			Uint8Array.of(1),
			Uint8Array.of(1, 0),
			Uint8Array.of(1, 1),
		];
		const layouts: Array<(v: KeyBytes) => Uint8Array> = [
			(v) => encodeGsiSortKey(v, k("u1"), k("s")),
			(v) => encodeGsiSortKey(k("open"), v, k("s")),
			(v) => encodeGsiSortKey(k("open"), k("u1"), v),
		];
		for (const [component, layout] of layouts.entries()) {
			const encoded = values.map((v) => KeyCodec.asKeyBytes(layout(k(v))));
			for (let i = 1; i < encoded.length; i++) {
				expect(KeyCodec.compare(encoded[i - 1], encoded[i]), `component ${component}, value ${i}`).toBeLessThan(0);
			}
		}
	});

	it("rejects bytes that are not a physical GSI sort key", () => {
		const bytes = (s: string) => new TextEncoder().encode(s);
		expect(() => decodeGsiSortKey(bytes("abc"))).toThrow();
		expect(() => decodeGsiSortKey(bytes("a\0\0b9:1"))).toThrow();
		// The body does not end with the third separator.
		expect(() => decodeGsiSortKey(bytes("a\0\0b\0\0c1:1"))).toThrow();
		// Only two separators before the end of the body.
		expect(() => decodeGsiSortKey(bytes("a\0\0b\0\x001:1"))).toThrow();
		// A flag other than ':' and '!'.
		expect(() => decodeGsiSortKey(bytes("a\0\0b\0\0c\0\x001;1"))).toThrow();
		expect(() => gsiEntryPrefixLength(bytes("a\0\0b\0\0c\0\x001;1"))).toThrow();
	});
});

describe("the order of GSI sort keys (property-based)", () => {
	type Part = string | Uint8Array | undefined;
	type Triple = [Part, Part, Part];

	// A defect of the order shows only when one part is a prefix of another part, for example "order"
	// and "order#01". Random parts from the full byte range almost never share a prefix. Thus each part
	// has at most 4 symbols from a small set, and many pairs share a prefix. Each symbol tests a risk:
	//
	// - 0x00: the escape writes it as 00 FF, and the separator is 00 00.
	// - 0x01: the smallest byte after 0x00. The upper query bound of a GSI sort key ends with 00 01.
	// - 0x23 ('#'): it is less than each digit of the trailer.
	// - 0x30 ('0') and 0x39 ('9'): the first and the last digit of the trailer.
	// - 0x3A (':'): the flag byte.
	// - 0xFE and 0xFF: 0xFF is the binary tag and the second byte of an escape. 0xFE is the byte below it.
	//
	// A string part has no 0x00 byte, because a string key cannot hold one. A binary part can hold it.
	// An absent part is the empty key.
	const part: fc.Arbitrary<Part> = fc.oneof(
		fc.constant(undefined),
		fc.string({ unit: fc.constantFrom("\x01", "#", "0", "9", ":", "a"), minLength: 1, maxLength: 4 }),
		fc.array(fc.constantFrom(0x00, 0x01, 0x23, 0x30, 0x3a, 0xfe, 0xff), { minLength: 1, maxLength: 4 }).map((b) => Uint8Array.from(b)),
	);
	const triple: fc.Arbitrary<Triple> = fc.tuple(part, part, part);
	const parts = ([g, h, s]: Triple) => [g, h, s].map((p) => KeyCodec.encodeOptional(p));
	const physical = (t: Triple) => {
		const [g, h, s] = parts(t);
		return KeyCodec.asKeyBytes(encodeGsiSortKey(g, h, s));
	};
	const stored = (t: Triple) => {
		const [g, h, s] = parts(t);
		return gsiItemSortKey(g, h, s);
	};
	/** The order of two triples, part by part, as KeyCodec.compare. */
	const reference = (x: Triple, y: Triple) => {
		const a = parts(x);
		const b = parts(y);
		return KeyCodec.compare(a[0], b[0]) || KeyCodec.compare(a[1], b[1]) || KeyCodec.compare(a[2], b[2]);
	};
	const firstDifference = (a: Uint8Array, b: Uint8Array) => {
		let i = 0;
		while (i < a.length && i < b.length && a[i] === b[i]) {
			i++;
		}
		return i;
	};

	it("sorts the stored keys in the part-by-part order of the triples", () => {
		fc.assert(
			fc.property(triple, triple, (x, y) => {
				expect(Math.sign(KeyCodec.compare(physical(x), physical(y)))).toBe(Math.sign(reference(x, y)));
			}),
			{ numRuns: propertyRuns(2_000) },
		);
	});

	it("ends the entry prefix after the third separator, and the entry prefix decides each comparison", () => {
		fc.assert(
			fc.property(triple, triple, (x, y) => {
				const a = physical(x);
				const b = physical(y);
				const [g, h, s] = parts(x);
				expect(gsiEntryPrefixLength(a)).toBe(escapeKey(g).length + escapeKey(h).length + escapeKey(s).length + 6);
				const prefixA = gsiEntryPrefixLength(a);
				const prefixB = gsiEntryPrefixLength(b);
				if (KeyCodec.compare(a.subarray(0, prefixA) as KeyBytes, b.subarray(0, prefixB) as KeyBytes) === 0) {
					expect(KeyCodec.compare(a, b)).toBe(0);
					return;
				}
				const i = firstDifference(a, b);
				expect(i).toBeLessThan(Math.min(prefixA, prefixB));
			}),
			{ numRuns: propertyRuns(2_000) },
		);
	});

	it("puts each range boundary between two stored keys inside the entry prefix of the upper key", () => {
		fc.assert(
			fc.property(triple, triple, (x, y) => {
				const a = stored(x);
				const b = stored(y);
				const cmp = KeyCodec.compare(a, b);
				fc.pre(cmp !== 0);
				const [lo, hi] = cmp < 0 ? [a, b] : [b, a];
				expect(KeyCodec.shortestSeparator(lo, hi).length).toBeLessThanOrEqual(1 + gsiEntryPrefixLength(hi.subarray(1)));
			}),
			{ numRuns: propertyRuns(2_000) },
		);
	});
});
