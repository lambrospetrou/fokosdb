import { describe, expect, it } from "vitest";
import { KeyCodec, type KeyBytes } from "../../sharding/key-codec.js";
import { decodeGsiSortKey, encodeGsiSortKey } from "./sort-key.js";

const k = (key: string | Uint8Array) => KeyCodec.encode(key);
const NONE = KeyCodec.encodeOptional(undefined);
const ascii = (bytes: Uint8Array) => new TextDecoder().decode(bytes);

describe("encodeGsiSortKey", () => {
	it("writes the escaped components, the separators, and the <L>:<D> trailer", () => {
		const physical = encodeGsiSortKey(k("2026-10-17"), k("u1"), k("o9"));
		expect(ascii(physical)).toBe("2026-10-17\0\0u1\0\0o910:2");
	});

	it("escapes each 0x00 byte and appends the raw GSI sort key with the '!' flag", () => {
		const gsiSK = k(Uint8Array.of(0x61, 0x00, 0x62));
		const physical = encodeGsiSortKey(gsiSK, k("u1"), k("o9"));
		expect([...physical]).toEqual([
			0xff, 0x61, 0x00, 0xff, 0x62, 0, 0, 0x75, 0x31, 0, 0, 0x6f, 0x39, 0xff, 0x61, 0x00, 0x62, 0x34, 0x21, 0x31,
		]);
	});

	it("writes 0:1 when the GSI has no sort key", () => {
		expect(ascii(encodeGsiSortKey(NONE, k("u1"), NONE))).toBe("\0\0u1\0\x000:1");
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

	it("rejects bytes that are not a physical GSI sort key", () => {
		expect(() => decodeGsiSortKey(new TextEncoder().encode("abc"))).toThrow();
		expect(() => decodeGsiSortKey(new TextEncoder().encode("a\0\0b9:1"))).toThrow();
	});
});
