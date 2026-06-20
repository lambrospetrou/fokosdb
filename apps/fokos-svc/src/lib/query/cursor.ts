import { KeyCodec, type KeyBytes } from "../partition-topology/key-codec.js";
import { hash64 } from "../hash-primitives.js";
import type { SkInterval } from "./sk-interval.js";

export const CURSOR_VERSION = 1;

export type DecodedCursor = {
	version: number;
	direction: "fwd" | "rev";
	fingerprint: bigint;
	queryIdx: number;
	inner: { hashKey: KeyBytes; sortKey: KeyBytes; inclusive: boolean } | null;
};

type CursorWire = {
	v: number;
	d: "fwd" | "rev";
	fp: string;
	qi: number;
	inner: { hk: string; sk: string; incl: boolean } | null;
};

export function encodeCursor(c: DecodedCursor): string {
	const wire: CursorWire = {
		v: c.version,
		d: c.direction,
		fp: c.fingerprint.toString(),
		qi: c.queryIdx,
		inner:
			c.inner === null
				? null
				: {
						hk: c.inner.hashKey.toBase64({ alphabet: "base64url" }),
						sk: c.inner.sortKey.toBase64({ alphabet: "base64url" }),
						incl: c.inner.inclusive,
					},
	};
	return new TextEncoder().encode(JSON.stringify(wire)).toBase64({ alphabet: "base64url" });
}

export function decodeCursor(s: string): DecodedCursor {
	let wire: CursorWire;
	try {
		wire = JSON.parse(new TextDecoder().decode(Uint8Array.fromBase64(s, { alphabet: "base64url" })));
	} catch {
		throw new Error("fokos/queryItems: cursor is not valid base64url-encoded JSON");
	}
	if (wire.v !== CURSOR_VERSION) throw new Error(`fokos/queryItems: unknown cursor version ${wire.v}`);
	if (wire.d !== "fwd" && wire.d !== "rev") throw new Error("fokos/queryItems: cursor has invalid direction");
	if (!Number.isSafeInteger(wire.qi) || wire.qi < 0) throw new Error("fokos/queryItems: cursor has invalid queryIdx");
	let fingerprint: bigint;
	try {
		fingerprint = BigInt(wire.fp);
	} catch {
		throw new Error("fokos/queryItems: cursor has invalid fingerprint");
	}
	let inner: DecodedCursor["inner"] = null;
	if (wire.inner !== null) {
		try {
			inner = {
				hashKey: KeyCodec.asKeyBytes(Uint8Array.fromBase64(wire.inner.hk, { alphabet: "base64url" })),
				sortKey: KeyCodec.asKeyBytes(Uint8Array.fromBase64(wire.inner.sk, { alphabet: "base64url" })),
				inclusive: !!wire.inner.incl,
			};
		} catch {
			throw new Error("fokos/queryItems: cursor has invalid inner resume position");
		}
	}
	return { version: wire.v, direction: wire.d, fingerprint, queryIdx: wire.qi, inner };
}

/**
 * Fingerprint over the request's identity-determining fields ONLY — the ordered sub-query list
 * (each hashKey + its normalized interval bounds/inclusivity + direction, with empty intervals
 * marked). Deliberately excludes limit/maxPageBytes/cursor, which may change between pages.
 */
export function computeCursorFingerprint(
	queries: Array<{ hashKey: KeyBytes; interval: SkInterval | null; direction: "asc" | "desc" }>,
): bigint {
	const sizeOfBound = (b: { value: KeyBytes } | undefined) => (b ? 1 + 4 + b.value.byteLength : 0);
	const sizeOfInterval = (q: (typeof queries)[number]) => {
		if (q.interval === null) return 1;
		return 1 + sizeOfBound(q.interval.lower) + 1 + sizeOfBound(q.interval.upper) + 1;
	};

	let total = 4;
	for (const q of queries) total += 4 + q.hashKey.byteLength + sizeOfInterval(q);

	const buf = new Uint8Array(total);
	const dv = new DataView(buf.buffer);
	let o = 0;

	const writeU8 = (n: number) => {
		buf[o++] = n;
	};
	const writeU32le = (n: number) => {
		dv.setUint32(o, n, true);
		o += 4;
	};
	const writeBytes = (src: Uint8Array) => {
		buf.set(src, o);
		o += src.byteLength;
	};

	writeU32le(queries.length);
	for (const q of queries) {
		writeU32le(q.hashKey.byteLength);
		writeBytes(q.hashKey);
		if (q.interval === null) {
			writeU8(2);
			continue;
		}
		const lo = q.interval.lower;
		const up = q.interval.upper;
		writeU8(lo ? 1 : 0);
		if (lo) {
			writeU8(lo.inclusive ? 1 : 0);
			writeU32le(lo.value.byteLength);
			writeBytes(lo.value);
		}
		writeU8(up ? 1 : 0);
		if (up) {
			writeU8(up.inclusive ? 1 : 0);
			writeU32le(up.value.byteLength);
			writeBytes(up.value);
		}
		writeU8(q.direction === "asc" ? 0 : 1);
	}
	return hash64(buf);
}
