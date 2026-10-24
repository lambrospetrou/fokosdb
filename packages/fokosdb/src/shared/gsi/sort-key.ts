/**
 * The physical sort key of a GSI entry: `escape(gsiSK) 00 00 escape(baseHK) 00 00 escape(baseSK) <trailer>`.
 *
 * `escape` writes each `0x00` byte as `0x00 0xFF`, so an escaped component never holds `0x00 0x00`. The
 * entries thus sort by `gsiSK`, then by `baseHK`, then by `baseSK`. The trailer is `<L><flag><D>`:
 * `<L>` is the decimal ASCII byte length of `gsiSK`, `<flag>` is `':'`, or `'!'` when `gsiSK` holds a
 * `0x00` byte and a raw copy of `gsiSK` comes before the trailer, and `<D>` is the digit count of `<L>`.
 * SQLite reads `gsiSK` back with `substr` and no scan. An absent `gsiSK` is the empty key.
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

/** The physical sort key of a GSI entry, without the binary tag that `KeyCodec.encode` adds. */
export function encodeGsiSortKey(gsiSK: KeyBytes, baseHK: KeyBytes, baseSK: KeyBytes): Uint8Array {
	const hasNul = gsiSK.includes(NUL);
	const length = new TextEncoder().encode(String(gsiSK.length));
	invariant(length.length <= 9, "fokos/gsi: the GSI sort key is too long");
	const parts = [escapeKey(gsiSK), SEPARATOR, escapeKey(baseHK), SEPARATOR, escapeKey(baseSK)];
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

/** The inverse of `encodeGsiSortKey`. Throws when the bytes are not a physical GSI sort key. */
export function decodeGsiSortKey(physicalSk: Uint8Array): GsiSortKeyParts {
	const n = physicalSk.length;
	invariant(n >= 3, "fokos/gsi: the physical sort key has no trailer");
	const d = physicalSk[n - 1] - DIGIT_0;
	const flag = physicalSk[n - 2];
	invariant(d >= 1 && d <= 9 && (flag === FLAG_PLAIN || flag === FLAG_RAW) && n >= 2 + d, "fokos/gsi: the trailer is not valid");
	let length = 0;
	for (let i = n - 2 - d; i < n - 2; i++) {
		const digit = physicalSk[i] - DIGIT_0;
		invariant(digit >= 0 && digit <= 9, "fokos/gsi: the trailer length is not a number");
		length = length * 10 + digit;
	}
	const bodyEnd = n - 2 - d - (flag === FLAG_RAW ? length : 0);
	invariant(bodyEnd >= 0, "fokos/gsi: the trailer length is too large");
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

const SEPARATOR = Uint8Array.of(NUL, NUL);
