/**
 * Measures the candidate encoders of one range DO name component inside workerd, over the 20 key formats of
 * docs/agent-plans/2026-09-27-range-do-name-text-encoding.md: each full key, and the boundary between it and a
 * close lower key. Run it with `pnpm --filter fokosdb bench:workerd`.
 *
 * Timers in workerd advance in whole milliseconds, so each sample times a batch of at least MIN_BATCH_MS.
 */
import { expect, it } from "vitest";
import { KeyCodec, type KeyBytes } from "../../src/sharding/key-codec.js";

const MIN_BATCH_MS = 200;
const SAMPLES = 7;

const hex = (h: string) => Uint8Array.fromHex(h);
// [kind, key, a close lower key]
const KEYS: [string, string | Uint8Array, string | Uint8Array][] = [
	["ULID", "01HRZ8J6X7Q2N3M4P5R6S7T8V9", "01HRZ8J6X4KQWE7T9YB2C3D4F5"],
	["UUIDv4 text", "f47ac10b-58cc-4372-a567-0e02b2c3d479", "f47ab2e1-0c3d-4f5e-8a9b-1c2d3e4f5a6b"],
	["UUID binary (16 B)", hex("f47ac10b58cc4372a5670e02b2c3d479"), hex("f47ab2e10c3d4f5e8a9b1c2d3e4f5a6b")],
	["KSUID", "2ZgXkLmNpQrStUvWxYz0AbCdEfG", "2ZgXkLmHbJ8kQ3wR5tY7uI9oP1a"],
	["Snowflake ID", "1767542397593645056", "1767542397589450752"],
	["ISO-8601 ms timestamp", "2024-03-15T14:30:00.123Z", "2024-03-15T14:29:59.987Z"],
	[
		"DynamoDB ORDER#ts#ULID",
		"ORDER#2024-03-15T14:30:00Z#01HRZ8J6X7Q2N3M4P5R6S7T8V9",
		"ORDER#2024-03-15T14:29:58Z#01HRZ8J4A1B2C3D4E5F6G7H8J9",
	],
	["DynamoDB USER#email", "USER#john.doe@example.com", "USER#jane.smith@example.com"],
	["DynamoDB hierarchy", "USA#WA#King#Seattle#98101", "USA#WA#King#Kirkland#98033"],
	["DynamoDB version prefix", "v0#INVOICE#2024-0315-00042", "v0#INVOICE#2024-0314-00917"],
	["Email", "john.doe+newsletter@example.com", "john.doe@example.com"],
	["Git SHA-1", "9fceb02d0ae598e95dc970b74767f19372d61af8", "9fceb01e3c2a1b0f9e8d7c6b5a4f3e2d1c0b9a88"],
	["S3 photo key", "photos/2024/03/15/IMG_20240315_143000.jpg", "photos/2024/03/15/IMG_20240315_142958.jpg"],
	[
		"S3 Hive parquet",
		"logs/year=2024/month=03/day=15/hour=14/part-00000-3f2a9c1e-7b4d-4e8f-9a1b-2c3d4e5f6a7b-c000.snappy.parquet",
		"logs/year=2024/month=03/day=15/hour=13/part-00011-9c8b7a6f-5e4d-4c3b-2a19-0f8e7d6c5b4a-c000.snappy.parquet",
	],
	[
		"S3 CloudTrail key",
		"AWSLogs/123456789012/CloudTrail/us-east-1/2024/03/15/123456789012_CloudTrail_us-east-1_20240315T1430Z_a1B2c3D4e5F6g7H8.json.gz",
		"AWSLogs/123456789012/CloudTrail/us-east-1/2024/03/15/123456789012_CloudTrail_us-east-1_20240315T1425Z_Zq9Xw8Vu7Ts6Rp5.json.gz",
	],
	["File name with spaces", "Q1 2024 Financial Report (Final).pdf", "Q1 2024 Board Deck v3.pptx"],
	["File name, German", "Präsentation März 2024.pptx", "Präsentation Februar 2024.pptx"],
	["File name, Japanese", "議事録_2024年3月.docx", "議事録_2024年2月.docx"],
	["npm package@version", "lodash@4.17.21", "lodash@4.17.20"],
	["URL path", "/api/v2/users/12345/orders/98765", "/api/v2/users/12345/orders/98712"],
];

// Today's encoder: a copy of `isSafeNameByte` and `encodeRangeComponent` in partition-id.ts.
function isSafeNameByte(b: number): boolean {
	if (b < 0x21 || b > 0x7d) {
		return false;
	}
	return b !== 0x22 && b !== 0x25 && b !== 0x2e && b !== 0x5c;
}
function percentLoop(bytes: Uint8Array): string {
	let out = "";
	for (const b of bytes) {
		out += isSafeNameByte(b) ? String.fromCharCode(b) : "%" + b.toString(16).padStart(2, "0").toUpperCase();
	}
	return out;
}

const HEX = Array.from({ length: 256 }, (_, b) => "%" + b.toString(16).padStart(2, "0").toUpperCase());
function percentTable(bytes: Uint8Array): string {
	let out = "";
	for (const b of bytes) {
		out += isSafeNameByte(b) ? String.fromCharCode(b) : HEX[b];
	}
	return out;
}

const base64url = (bytes: Uint8Array) => bytes.toBase64({ alphabet: "base64url", omitPadding: true });

// The escape set of the RFC, as fixed code point ranges.
const ESCAPE_SOURCE =
	"[\\u0000-\\u0020\\u0022\\u0025\\u002e\\u005c\\u007e-\\u009f\\u200b-\\u200f\\u2028\\u2029\\u202a-\\u202e\\u2066-\\u2069\\ufeff]";
const ESCAPE_TEST = new RegExp(ESCAPE_SOURCE);
const ESCAPE_ALL = new RegExp(ESCAPE_SOURCE, "g");
const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
const encoder = new TextEncoder();

// Every code point of the escape set is below U+10000, so it has 1 to 3 UTF-8 bytes.
function escapeWithTable(c: string): string {
	const cp = c.charCodeAt(0);
	if (cp < 0x80) {
		return HEX[cp];
	}
	if (cp < 0x800) {
		return HEX[0xc0 | (cp >> 6)] + HEX[0x80 | (cp & 0x3f)];
	}
	return HEX[0xe0 | (cp >> 12)] + HEX[0x80 | ((cp >> 6) & 0x3f)] + HEX[0x80 | (cp & 0x3f)];
}
function escapeWithEncoder(c: string): string {
	let out = "";
	for (const b of encoder.encode(c)) {
		out += HEX[b];
	}
	return out;
}

/** The start of the incomplete UTF-8 sequence at the end of `bytes`, or `bytes.length` when there is none. */
function tailStart(bytes: Uint8Array): number {
	for (let i = bytes.length - 1; i >= Math.max(0, bytes.length - 3); i--) {
		const b = bytes[i];
		if ((b & 0xc0) === 0x80) {
			continue;
		}
		// 0 for a byte that starts no sequence (0xF8 to 0xFF): the tail is empty, and the decode fails.
		const need = b < 0x80 ? 1 : b >= 0xc0 && b <= 0xdf ? 2 : b >= 0xe0 && b <= 0xef ? 3 : b >= 0xf0 && b <= 0xf7 ? 4 : 0;
		return bytes.length - i < need ? i : bytes.length;
	}
	return bytes.length;
}

/** Encoder A: a `subarray` view for each call, and a regular-expression replacement. */
function encoderA(bytes: Uint8Array): string {
	if (bytes[0] === 0xff) {
		return "~b" + base64url(bytes);
	}
	const tail = tailStart(bytes);
	let text: string;
	try {
		text = decoder.decode(bytes.subarray(0, tail));
	} catch {
		return "~b" + base64url(bytes);
	}
	if (ESCAPE_TEST.test(text)) {
		text = text.replace(ESCAPE_ALL, escapeWithTable);
	}
	for (let i = tail; i < bytes.length; i++) {
		text += HEX[bytes[i]];
	}
	return text;
}

/** True when the code unit is in the escape set. */
function isEscaped(cp: number): boolean {
	if (cp < 0xa0) {
		return cp <= 0x20 || cp >= 0x7e || cp === 0x22 || cp === 0x25 || cp === 0x2e || cp === 0x5c;
	}
	return (
		(cp >= 0x200b && cp <= 0x200f) ||
		cp === 0x2028 ||
		cp === 0x2029 ||
		(cp >= 0x202a && cp <= 0x202e) ||
		(cp >= 0x2066 && cp <= 0x2069) ||
		cp === 0xfeff
	);
}

/** Escapes each character of the escape set, and appends the text between two escapes as one slice. */
function escapeScan(text: string): string {
	let out = "";
	let last = 0;
	for (let i = 0; i < text.length; i++) {
		const cp = text.charCodeAt(i);
		if (isEscaped(cp)) {
			out += text.slice(last, i) + escapeWithTable(text[i]);
			last = i + 1;
		}
	}
	return last === 0 ? text : out + text.slice(last);
}

/** Encoder B: a view only for an incomplete tail, and a regular-expression replacement. */
function encoderB(bytes: Uint8Array): string {
	if (bytes[0] === 0xff) {
		return "~b" + base64url(bytes);
	}
	const tail = tailStart(bytes);
	let text: string;
	try {
		text = decoder.decode(tail === bytes.length ? bytes : bytes.subarray(0, tail));
	} catch {
		return "~b" + base64url(bytes);
	}
	if (ESCAPE_TEST.test(text)) {
		text = text.replace(ESCAPE_ALL, escapeWithTable);
	}
	for (let i = tail; i < bytes.length; i++) {
		text += HEX[bytes[i]];
	}
	return text;
}

/** Encoder C: a view only for an incomplete tail, and a scan of each component. */
function encoderC(bytes: Uint8Array): string {
	if (bytes[0] === 0xff) {
		return "~b" + base64url(bytes);
	}
	const tail = tailStart(bytes);
	let text: string;
	try {
		text = decoder.decode(tail === bytes.length ? bytes : bytes.subarray(0, tail));
	} catch {
		return "~b" + base64url(bytes);
	}
	text = escapeScan(text);
	for (let i = tail; i < bytes.length; i++) {
		text += HEX[bytes[i]];
	}
	return text;
}

/** The encoder of the RFC: a view only for an incomplete tail, a test, and a scan only on a match. */
function encodeComponent(bytes: Uint8Array): string {
	if (bytes[0] === 0xff) {
		return "~b" + base64url(bytes);
	}
	const tail = tailStart(bytes);
	let text: string;
	try {
		text = decoder.decode(tail === bytes.length ? bytes : bytes.subarray(0, tail));
	} catch {
		return "~b" + base64url(bytes);
	}
	if (ESCAPE_TEST.test(text)) {
		text = escapeScan(text);
	}
	for (let i = tail; i < bytes.length; i++) {
		text += HEX[bytes[i]];
	}
	return text;
}

const OPERATIONS: [string, (bytes: Uint8Array) => string][] = [
	["Percent-encoding, today's loop", percentLoop],
	["Percent-encoding with the hex table", percentTable],
	["base64url (`toBase64`)", base64url],
	["Decode only", (bytes) => decoder.decode(bytes)],
	[
		"Decode, then a test",
		(bytes) => {
			const text = decoder.decode(bytes);
			ESCAPE_TEST.test(text);
			return text;
		},
	],
	["Decode, then a regular-expression replacement", (bytes) => decoder.decode(bytes).replace(ESCAPE_ALL, escapeWithTable)],
	["Decode, then a replacement with `TextEncoder`", (bytes) => decoder.decode(bytes).replace(ESCAPE_ALL, escapeWithEncoder)],
	["Decode, then a scan", (bytes) => escapeScan(decoder.decode(bytes))],
	["Encoder A: view always, replacement", encoderA],
	["Encoder B: view for a tail only, replacement", encoderB],
	["Encoder C: view for a tail only, scan always", encoderC],
	["Encoder of the RFC: view for a tail only, test, scan on a match", encodeComponent],
];

/** The median ns per component of `fn` over `components`. */
function measure(fn: (bytes: Uint8Array) => string, components: Uint8Array[]): number {
	let sink = 0;
	const run = (rounds: number) => {
		for (let r = 0; r < rounds; r++) {
			for (const c of components) {
				sink += fn(c).length;
			}
		}
	};
	// Warm up, and find the number of rounds that takes at least MIN_BATCH_MS.
	let rounds = 1_000;
	for (;;) {
		const start = performance.now();
		run(rounds);
		if (performance.now() - start >= MIN_BATCH_MS) {
			break;
		}
		rounds *= 2;
	}
	const samples: number[] = [];
	for (let s = 0; s < SAMPLES; s++) {
		const start = performance.now();
		run(rounds);
		samples.push(((performance.now() - start) * 1e6) / (rounds * components.length));
	}
	expect(sink).toBeGreaterThan(0);
	samples.sort((a, b) => a - b);
	return samples[Math.floor(SAMPLES / 2)];
}

it("range DO name component encoders", () => {
	const components: KeyBytes[] = [];
	for (const [, key, lower] of KEYS) {
		const hi = KeyCodec.encode(key);
		components.push(hi, KeyCodec.shortestSeparator(KeyCodec.encode(lower), hi));
	}
	// The groups follow the paths of the RFC encoder: text with nothing to escape, text with an escape or an
	// incomplete tail, and binary keys.
	const decodable = (c: Uint8Array) => c[0] !== 0xff && tailStart(c) === c.length;
	const binary = components.filter((c) => c[0] === 0xff);
	const text = components.filter((c) => c[0] !== 0xff);
	const plain = text.filter((c) => decodable(c) && !ESCAPE_TEST.test(decoder.decode(c)));
	const escaped = text.filter((c) => !plain.includes(c));
	// The tail rule: an incomplete sequence gives a %XX tail, a byte that starts no sequence gives the ~b form,
	// and a complete 4-byte character at the end gives an empty tail.
	expect(encodeComponent(new Uint8Array([0x61, 0xe8]))).toBe("a%E8");
	expect(encodeComponent(new Uint8Array([0x61, 0xf8]))).toBe("~bYfg");
	expect(encodeComponent(KeyCodec.encode("a\u{1F389}"))).toBe("a\u{1F389}");
	// All encoders give the same names, and each ASCII component keeps its name of today.
	for (const c of components) {
		const name = encodeComponent(c);
		expect([encoderA(c), encoderB(c), encoderC(c)]).toEqual([name, name, name]);
		if (c.every((b) => b < 0x80)) {
			expect(name).toBe(percentLoop(c));
		}
	}

	const groups: [string, Uint8Array[]][] = [
		[`No escapes (${plain.length})`, plain],
		[`With escapes (${escaped.length})`, escaped],
		[`Binary (${binary.length})`, binary],
	];
	const rows = OPERATIONS.map(([name, fn]) => {
		// A "Decode" row decodes the whole component, so it applies only to text with no incomplete tail.
		const cells = groups.map(([, set]) => {
			const usable = name.startsWith("Decode") ? set.filter(decodable) : set;
			return usable.length === 0 ? "–" : measure(fn, usable).toFixed(0);
		});
		return `| ${name} | ${cells.join(" | ")} |`;
	});
	const avg = (set: Uint8Array[]) => Math.round(set.reduce((a, c) => a + c.length, 0) / set.length);
	console.log(
		[
			`workerd, ns per component, median of ${SAMPLES} batches of at least ${MIN_BATCH_MS} ms`,
			`| Operation | ${groups.map(([g, set]) => `${g}, avg ${avg(set)} B`).join(" | ")} |`,
			`| --- | ${groups.map(() => "---:").join(" | ")} |`,
			...rows,
		].join("\n"),
	);
});
