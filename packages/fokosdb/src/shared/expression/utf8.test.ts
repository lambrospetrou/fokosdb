import { describe, expect, it } from "vitest";
import { utf8ByteLength } from "./utf8.js";

describe("utf8ByteLength", () => {
	it.each([
		[0, "empty text", ""],
		[14, "ASCII", "plain text 123"],
		[15, "2-byte characters", "naïve café ñ"],
		[13, "3-byte characters", "€ 日本語"],
		[13, "4-byte characters", "😀 and 𝄞"],
		[25, "the limits of each width", "\u007f\u0080߿ࠀ퟿￿\u{10000}\u{10ffff}"],
	] as const)("gives %i bytes for %s", (bytes, _name, text) => {
		expect(utf8ByteLength(text)).toBe(bytes);
	});

	it.each([
		["a high surrogate before text", "a\ud800b", 5],
		["a high surrogate at the end", "a\ud800", 4],
		["a low surrogate with no high surrogate", "\udc00a", 4],
		["a low surrogate before a high surrogate", "\udc00\ud800", 6],
		["two high surrogates before a low surrogate", "\ud800\ud83d\ude00", 7],
	])("counts a surrogate that has no partner as 3 bytes: %s", (_name, text, bytes) => {
		expect(utf8ByteLength(text)).toBe(bytes);
	});

	it("counts a text that is larger than its internal buffer, with a 4-byte character at each buffer limit", () => {
		// Each length puts the start of a 4-byte character at a different place near the 16 KiB limit.
		for (const padding of [0, 1, 2, 3, 4, 5]) {
			const text = "a".repeat(padding) + "😀".repeat(20_000) + "€".repeat(20_000) + "é".repeat(20_000) + "z".repeat(20_000);
			expect(utf8ByteLength(text), `padding ${padding}`).toBe(padding + 80_000 + 60_000 + 40_000 + 20_000);
		}
		expect(utf8ByteLength("\ud800".repeat(50_000))).toBe(150_000);
	});

	it("gives the byte count of the encoder for each code unit and for each pair with the next one", () => {
		const encoder = new TextEncoder();
		for (let unit = 0; unit <= 0xffff; unit += 37) {
			for (const next of ["", "a", "\udc00", "\ud800"]) {
				const text = String.fromCharCode(unit) + next;
				expect(utf8ByteLength(text), `U+${unit.toString(16)} then ${JSON.stringify(next)}`).toBe(encoder.encode(text).byteLength);
			}
		}
	});
});
