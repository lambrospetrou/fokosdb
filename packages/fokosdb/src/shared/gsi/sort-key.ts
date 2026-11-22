/**
 * The physical sort key of a GSI entry:
 * `escape(gsiSK) 00 00 escape(baseHK) 00 00 escape(baseSK) 00 00 [gsiSK] <L><flag><D>`.
 *
 * `escape` writes each `0x00` byte as `0x00 0xFF`, so an escaped component never holds `0x00 0x00`. The
 * entry prefix is the bytes up to the end of the third separator, and it identifies the entry. Each
 * component ends with `00 00`, and `00 00` is less than each byte that can follow it in a longer
 * component. Thus the entries sort by `gsiSK`, then by `baseHK`, then by `baseSK`, each compared as
 * `KeyBytes`, and the comparison of two different entries ends inside the entry prefix.
 *
 * The suffix is the optional raw copy of `gsiSK` and the trailer `<L><flag><D>`. It is a function of
 * `gsiSK` only, so it never decides the order. `<L>` is the decimal ASCII byte length of `gsiSK`, `<flag>`
 * is `':'`, or `'!'` when `gsiSK` holds a `0x00` byte and a raw copy of `gsiSK` comes before `<L>`, and
 * `<D>` is the digit count of `<L>`. SQLite reads `gsiSK` back with `substr` and no scan. An absent
 * `gsiSK` is the empty key.
 *
 * The flag also identifies the layout. A new layout uses new flag values, and a flag value is never an
 * ASCII digit. The decoder refuses an unknown flag. `GSI_SORT_KEY_VALUE` in `expression/compiler.ts` reads
 * each flag other than `'!'` as `':'`, so a new flag value also changes that expression.
 */
import { KeyCodec, type KeyBytes } from "../../sharding/key-codec.js";
import invariant from "../invariant.js";

const NUL = 0x00;
const ESCAPED_NUL = 0xff;
const FLAG_PLAIN = 0x3a; // ':'
const FLAG_RAW = 0x21; // '!'
const DIGIT_0 = 0x30;

export type GsiSortKeyParts = { gsiSK: KeyBytes; baseHK: KeyBytes; baseSK: KeyBytes };

/** The escaped bytes of a key: each `0x00` becomes `0x00 0xFF`. */
export function escapeKey(key: Uint8Array): Uint8Array {
	let nuls = 0;
	for (const b of key) {
		if (b === NUL) {
			nuls++;
		}
	}
	if (nuls === 0) {
		return key;
	}
	const out = new Uint8Array(key.length + nuls);
	let j = 0;
	for (const b of key) {
		out[j++] = b;
		if (b === NUL) {
			out[j++] = ESCAPED_NUL;
		}
	}
	return out;
}

/**
 * The physical sort key of a GSI entry, without the binary tag that `KeyCodec.encode` adds. For example,
 * the text keys `gsiSK = "2026"`, `baseHK = "u1"`, and `baseSK = "o9"` give
 * `"2026" 00 00 "u1" 00 00 "o9" 00 00 "4" ":" "1"`. `decodeGsiSortKey` describes the full layout.
 */
export function encodeGsiSortKey(gsiSK: KeyBytes, baseHK: KeyBytes, baseSK: KeyBytes): Uint8Array {
	const hasNul = gsiSK.includes(NUL);
	const length = digitBytes(gsiSK.length);
	invariant(length.length <= 9, "fokos/gsi: the GSI sort key is too long");
	const parts = [escapeKey(gsiSK), SEPARATOR, escapeKey(baseHK), SEPARATOR, escapeKey(baseSK), SEPARATOR];
	if (hasNul) {
		parts.push(gsiSK);
	}
	parts.push(length, Uint8Array.of(hasNul ? FLAG_RAW : FLAG_PLAIN, DIGIT_0 + length.length));
	const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
	let at = 0;
	for (const p of parts) {
		out.set(p, at);
		at += p.length;
	}
	return out;
}

/** The stored sort key of a GSI entry: `0xFF || encodeGsiSortKey(...)`. */
export function gsiItemSortKey(gsiSK: KeyBytes, baseHK: KeyBytes, baseSK: KeyBytes): KeyBytes {
	return KeyCodec.encode(encodeGsiSortKey(gsiSK, baseHK, baseSK));
}

/**
 * The length of the entry prefix of a physical GSI sort key: the bytes up to the end of the third
 * separator. It takes the bytes without the binary tag, as `decodeGsiSortKey` does. Throws when the
 * trailer is not valid.
 *
 * 1. The last byte is `<D>`, the digit count of `<L>`, and the byte before it is `<flag>`.
 * 2. The `<D>` bytes before `<flag>` are `<L>`, the byte length of `gsiSK` in decimal ASCII.
 * 3. With the `'!'` flag, the `<L>` bytes before `<L>` are a raw copy of `gsiSK`. The entry prefix ends
 *    before them. With the `':'` flag, the entry prefix ends at `<L>`.
 *
 * Exported for testing.
 */
export function gsiEntryPrefixLength(physicalSk: Uint8Array): number {
	return readTrailer(physicalSk).prefixLength;
}

/**
 * The inverse of `encodeGsiSortKey`. Throws when the bytes are not a physical GSI sort key.
 *
 * The layout, with `n` bytes in total:
 *
 *   escape(gsiSK) 00 00 escape(baseHK) 00 00 escape(baseSK) 00 00 [gsiSK] <L> <flag> <D>
 *   └────────────────────── entry prefix ───────────────────────┘ └────── suffix ──────┘
 *
 * `gsiEntryPrefixLength` reads the suffix from the end. The entry prefix ends with `00 00`. The bytes
 * before it are read from the start: `00 FF` is an escaped `0x00` byte of a component, and `00 00`
 * ends a component. They hold three components: `gsiSK`, `baseHK`, and `baseSK`.
 */
export function decodeGsiSortKey(physicalSk: Uint8Array): GsiSortKeyParts {
	const { prefixLength, length } = readTrailer(physicalSk);
	invariant(
		prefixLength >= 2 && physicalSk[prefixLength - 2] === NUL && physicalSk[prefixLength - 1] === NUL,
		"fokos/gsi: the entry prefix does not end with a separator",
	);
	const bodyEnd = prefixLength - 2;
	const components: KeyBytes[] = [];
	let current: number[] = [];
	for (let i = 0; i < bodyEnd; i++) {
		const b = physicalSk[i];
		if (b !== NUL) {
			current.push(b);
			continue;
		}
		const next = physicalSk[++i];
		if (next === ESCAPED_NUL) {
			current.push(NUL);
			continue;
		}
		invariant(next === NUL && i < bodyEnd, "fokos/gsi: the physical sort key has a bad escape");
		components.push(KeyCodec.asKeyBytes(Uint8Array.from(current)));
		current = [];
	}
	components.push(KeyCodec.asKeyBytes(Uint8Array.from(current)));
	invariant(components.length === 3, "fokos/gsi: the physical sort key does not have three components");
	const [gsiSK, baseHK, baseSK] = components;
	invariant(gsiSK.length === length, "fokos/gsi: the trailer length does not match the GSI sort key");
	return { gsiSK, baseHK, baseSK };
}

/** The steps of `gsiEntryPrefixLength`. `length` is `<L>`, the byte length of `gsiSK`. */
function readTrailer(physicalSk: Uint8Array): { prefixLength: number; length: number } {
	const n = physicalSk.length;
	// Step 1: the last two bytes are <D> and <flag>.
	invariant(n >= 3, "fokos/gsi: the physical sort key has no trailer");
	const d = physicalSk[n - 1] - DIGIT_0;
	const flag = physicalSk[n - 2];
	invariant(d >= 1 && d <= 9 && (flag === FLAG_PLAIN || flag === FLAG_RAW) && n >= 2 + d, "fokos/gsi: the trailer is not valid");
	// Step 2: <L> is the <D> digits before <flag>.
	let length = 0;
	for (let i = n - 2 - d; i < n - 2; i++) {
		const digit = physicalSk[i] - DIGIT_0;
		invariant(digit >= 0 && digit <= 9, "fokos/gsi: the trailer length is not a number");
		length = length * 10 + digit;
	}
	// Step 3: the entry prefix ends before <L>, or before the raw copy of gsiSK with the '!' flag.
	const prefixLength = n - 2 - d - (flag === FLAG_RAW ? length : 0);
	invariant(prefixLength >= 0, "fokos/gsi: the trailer length is too large");
	return { prefixLength, length };
}

const SEPARATOR = Uint8Array.of(NUL, NUL);

/**
 * The decimal ASCII digits of a non-negative integer, most significant first, with no string in between.
 * For example, 0 gives `[0x30]` ("0"), 7 gives `[0x37]` ("7"), and 512 gives `[0x35, 0x31, 0x32]` ("512").
 */
function digitBytes(n: number): Uint8Array {
	const out = new Uint8Array(n === 0 ? 1 : Math.floor(Math.log10(n)) + 1);
	for (let i = out.length - 1; i >= 0; i--, n = Math.floor(n / 10)) {
		out[i] = DIGIT_0 + (n % 10);
	}
	return out;
}
