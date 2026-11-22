import { asRoutingHash, hash64, inRoutingHashInterval, routingHashBytes, type RoutingHash } from "../../sharding/hash-primitives.js";
import { KeyCodec, type KeyBytes } from "../../sharding/key-codec.js";
import { FokosValidationError, VALIDATION_CODES } from "../errors.js";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export const SCAN_CURSOR_VERSION = 1;

/** One segment of a parallel scan, or null for the whole table. */
export type ScanSegment = { segment: number; totalSegments: number } | null;

/** The routingHash interval `[start, end)` of a segment. A null `end` has no upper bound. */
export type SegmentBounds = { start: RoutingHash; end: RoutingHash | null };

/** The position where a page starts: at `pos` when `inclusive` is true, else after it. */
export type ScanResume = { pos: { routingHash: RoutingHash; hk?: KeyBytes; sk?: KeyBytes }; inclusive: boolean };

type ScanCursorWire = {
	v: number;
	k: "scan";
	/** The u64 fingerprint as a decimal string. A JSON number is exact only to 2^53. */
	fp: string;
	pos: { rh: string; hk?: string; sk?: string };
	incl: boolean;
};

function cursorMalformed(message: string, cause?: unknown): FokosValidationError {
	return new FokosValidationError(VALIDATION_CODES.cursor_malformed, { message, cause });
}

/**
 * Segment `s` of `T` is `[⌊s · 2^64 / T⌋, ⌊(s + 1) · 2^64 / T⌋)`, as 8 bytes with the most significant byte
 * first. The last segment has no upper bound. The edges do not depend on the partition tree.
 */
export function segmentBounds(segment: ScanSegment): SegmentBounds {
	if (segment === null) {
		return { start: routingHashBytes(0n), end: null };
	}
	const total = BigInt(segment.totalSegments);
	const s = BigInt(segment.segment);
	return {
		start: routingHashBytes((s << 64n) / total),
		end: s + 1n === total ? null : routingHashBytes(((s + 1n) << 64n) / total),
	};
}

/** True when `routingHash` is inside the segment. */
export function inSegment(routingHash: RoutingHash, bounds: SegmentBounds): boolean {
	return inRoutingHashInterval(routingHash, bounds.start, bounds.end);
}

/**
 * `hash64` of the shard group of the table or the GSI, and of the segment. It does not cover the filter,
 * the projection, or the budgets, because a position is valid with each of them. The bytes are a u32le
 * length and the UTF-8 shard group, then the byte 0 for no segment, or the byte 1, the u32le `segment`,
 * and the u32le `totalSegments`.
 */
export function scanFingerprint(shardGroup: string, segment: ScanSegment): bigint {
	const group = encoder.encode(shardGroup);
	const buf = new Uint8Array(4 + group.byteLength + (segment === null ? 1 : 9));
	const dv = new DataView(buf.buffer);
	dv.setUint32(0, group.byteLength, true);
	buf.set(group, 4);
	const o = 4 + group.byteLength;
	if (segment !== null) {
		buf[o] = 1;
		dv.setUint32(o + 1, segment.segment, true);
		dv.setUint32(o + 5, segment.totalSegments, true);
	}
	return hash64(buf);
}

const b64 = (bytes: Uint8Array) => bytes.toBase64({ alphabet: "base64url" });

/** The opaque cursor of a page. A caller must not parse it or build it. */
export function encodeScanCursor(fingerprint: bigint, resume: ScanResume): string {
	const { routingHash, hk, sk } = resume.pos;
	const wire: ScanCursorWire = {
		v: SCAN_CURSOR_VERSION,
		k: "scan",
		fp: fingerprint.toString(),
		pos: { rh: b64(routingHash), ...(hk === undefined ? {} : { hk: b64(hk) }), ...(sk === undefined ? {} : { sk: b64(sk) }) },
		incl: resume.inclusive,
	};
	return b64(encoder.encode(JSON.stringify(wire)));
}

/**
 * The resume position of a cursor, after five checks in this order: the cursor is base64url JSON of an
 * object, its version is known, its fields are valid, its fingerprint is the fingerprint of the request,
 * and its position is inside the segment.
 */
export function decodeScanCursor(cursor: string, fingerprint: bigint, bounds: SegmentBounds): ScanResume {
	let parsed: unknown;
	try {
		parsed = JSON.parse(decoder.decode(Uint8Array.fromBase64(cursor, { alphabet: "base64url" })));
	} catch (e) {
		throw cursorMalformed("cursor is not valid base64url-encoded JSON", e);
	}
	if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
		throw cursorMalformed("cursor is not valid base64url-encoded JSON");
	}
	const wire = parsed as Partial<ScanCursorWire>;
	if (wire.v !== SCAN_CURSOR_VERSION) {
		throw new FokosValidationError(VALIDATION_CODES.cursor_version_unknown, {
			message: "unknown cursor version",
			attributes: { version: wire.v },
		});
	}
	if (
		wire.k !== "scan" ||
		typeof wire.incl !== "boolean" ||
		typeof wire.fp !== "string" ||
		typeof wire.pos !== "object" ||
		wire.pos === null
	) {
		throw cursorMalformed("cursor is not a scanItems cursor");
	}
	const { rh, hk, sk } = wire.pos;
	let resume: ScanResume;
	let fp: bigint;
	try {
		fp = BigInt(wire.fp);
		const bytes = (s: unknown) => {
			if (typeof s !== "string") {
				throw new Error("a position field is not a string");
			}
			return Uint8Array.fromBase64(s, { alphabet: "base64url" });
		};
		const routingHash = bytes(rh);
		if (routingHash.byteLength !== 8 || (sk !== undefined && hk === undefined)) {
			throw new Error("the position is not valid");
		}
		resume = {
			pos: {
				routingHash: asRoutingHash(routingHash),
				...(hk === undefined ? {} : { hk: KeyCodec.asKeyBytes(bytes(hk)) }),
				...(sk === undefined ? {} : { sk: KeyCodec.asKeyBytes(bytes(sk)) }),
			},
			inclusive: wire.incl,
		};
	} catch (e) {
		throw cursorMalformed("cursor has an invalid position or fingerprint", e);
	}
	if (fp !== fingerprint) {
		throw new FokosValidationError(VALIDATION_CODES.cursor_fingerprint_mismatch, {
			message: "cursor fingerprint mismatch — send the cursor with the same table, segment, and totalSegments",
		});
	}
	if (!inSegment(resume.pos.routingHash, bounds)) {
		throw cursorMalformed("cursor position is outside the segment");
	}
	return resume;
}
