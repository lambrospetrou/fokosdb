/**
 * KeyCodec — the single owner of key bytes and key ordering.
 *
 * See docs/ideas/key-ordering-utf8-convergence.md (the SPEC) for the *why* and every decision
 * (0xFF-tag binary, [] for absent key, reject empty/lone-surrogate, never-compare-raw invariant),
 * and docs/agent-plans/key-codec-bytes-implementation.md for the cutover plan.
 *
 * The contract in one line: encode at entry, decode at exit, compare bytes in between. No other code
 * fabricates, compares, or synthesizes keys — comparisons/synthesis go ONLY through this module.
 *
 * `KeyBytes` is a branded `Uint8Array`: structurally a `Uint8Array`, but nominally distinct so a raw
 * `string`/`Uint8Array` cannot be passed where an encoded key is required. The ONLY producers are
 * `encode`/`encodeOptional` (real work) and `asKeyBytes` (trusted re-brand for already-encoded buffers
 * from RPC and BLOB reads — a zero-cost cast).
 */

declare const KEY_BRAND: unique symbol;
export type KeyBytes = Uint8Array & { readonly [KEY_BRAND]: true };

// The 0xFF lead tags binary keys. UTF-8 never contains a 0xFF byte, so a string can never start with
// 0xFF and the tag is unambiguous without escaping. See SPEC "Type tagging".
const BINARY_TAG = 0xff;

// Stateless singletons: .encode()/.decode() each return a fresh buffer/string and hold no cross-call
// state, so sharing one instance is safe and saves a per-call allocation. (We never use the stateful
// encodeInto/streaming-decode modes.)
const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

/**
 * Trusted, zero-cost re-brand of an already-encoded buffer to `KeyBytes`. No copy, no validation.
 * Used for RPC payloads (the brand is erased over the wire) and BLOB reads (canonical bytes from
 * storage). Never `.slice()`/`Uint8Array.from()` here — that would silently make it a copy. `encode`
 * is the only path that does real work.
 */
function asKeyBytes(bytes: Uint8Array): KeyBytes {
	return bytes as unknown as KeyBytes;
}

/**
 * Public key → canonical KeyBytes. Strings encode to raw UTF-8 (no tag); binary to `0xFF || rawBytes`.
 *
 * Validation (SPEC decisions): rejects empty string, empty `Uint8Array`, and any string that is not
 * well-formed UTF-16 (a lone/unpaired surrogate — `TextEncoder` would silently rewrite it to U+FFFD).
 * Absent keys never reach here; they come from `encodeOptional(undefined) → []`. So `encode` never
 * returns `[]`.
 */
function encode(key: string | Uint8Array): KeyBytes {
	if (typeof key === "string") {
		if (key.length === 0) {
			throw new Error("fokos/KeyCodec.encode: empty string key is not allowed");
		}
		// isWellFormed() is false iff the string contains a lone surrogate (invalid UTF-16).
		if (key.isWellFormed?.() === false) {
			throw new Error("fokos/KeyCodec.encode: key string contains a lone surrogate (not well-formed UTF-16)");
		}
		return asKeyBytes(textEncoder.encode(key));
	}
	if (key.byteLength === 0) {
		throw new Error("fokos/KeyCodec.encode: empty binary key is not allowed");
	}
	const out = new Uint8Array(key.byteLength + 1);
	out[0] = BINARY_TAG;
	out.set(key, 1);
	return asKeyBytes(out);
}

/**
 * Maps an optional public key to KeyBytes: `undefined` → `[]` (the absent sentinel, the global byte
 * minimum), anything else → `encode(key)`. This is the ONLY producer of `[]`.
 */
function encodeOptional(key: string | Uint8Array | undefined): KeyBytes {
	return key === undefined ? asKeyBytes(new Uint8Array(0)) : encode(key);
}

/**
 * Canonical bytes → original-typed key. First byte `0xFF` ⇒ binary (the rest, untagged); else UTF-8.
 * `decode(encode(k)) === k` for every well-formed input. `decode([])` is `""` (the absent sentinel
 * never carries a meaningful decoded value; callers treat absent specially).
 */
function decode(k: KeyBytes): string | Uint8Array {
	if (k.length > 0 && k[0] === BINARY_TAG) {
		return k.slice(1);
	}
	return textDecoder.decode(k);
}

/** Unsigned byte compare (a memcmp). Produces the same total order as SQLite BLOB/memcmp. */
function compare(a: KeyBytes, b: KeyBytes): number {
	const minLen = Math.min(a.length, b.length);
	for (let i = 0; i < minLen; i++) {
		if (a[i] !== b[i]) {
			return a[i] - b[i];
		}
	}
	return a.length - b.length;
}

/**
 * Prefix upper bound in byte space: the least key that is a strict upper bound of every key having
 * `prefix` as a prefix. Increment the last byte `< 0xFF` and drop everything after it. Returns `null`
 * (unbounded) when no such byte exists — i.e. `prefix` is empty or all `0xFF`. Used for `beginsWith`
 * / prefix upper bounds.
 */
function successor(prefix: KeyBytes): KeyBytes | null {
	for (let i = prefix.length - 1; i >= 0; i--) {
		if (prefix[i] < 0xff) {
			const out = prefix.slice(0, i + 1);
			out[i] += 1;
			return asKeyBytes(out);
		}
	}
	return null;
}

/**
 * Byte-space shortest separator: the shortest `r` with `lo < r <= hi` in byte order (byte port of the
 * old UTF-16 `shortestSeparator`). Walk to the first differing byte `i` and return `hi[0..i+1]`; if
 * `lo` is a proper prefix of `hi`, return `hi[0..lo.length+1]`. Pre-condition: `compare(lo, hi) < 0`.
 * The only synthesis primitive that combines two keys; it never special-cases the `0xFF` tag.
 */
function shortestSeparator(lo: KeyBytes, hi: KeyBytes): KeyBytes {
	const minLen = Math.min(lo.length, hi.length);
	for (let i = 0; i < minLen; i++) {
		if (lo[i] !== hi[i]) {
			// hi[i] > lo[i] (pre-condition lo < hi), so hi[0..i+1] is the shortest prefix of hi exceeding lo.
			return asKeyBytes(hi.slice(0, i + 1));
		}
	}
	// lo is a proper prefix of hi; one extra byte makes the result exceed lo.
	return asKeyBytes(hi.slice(0, lo.length + 1));
}

/**
 * Human-readable rendering of a key for logs and error messages. Valid UTF-8
 * strings render as `"text"`; binary (0xFF-tagged) and the empty sentinel render as `hex:...`. Never
 * used for comparison or identity — display only. Raw `KeyBytes` must never print as a bare array.
 */
function keyForLog(k: KeyBytes): string {
	if (k.length === 0) return "<empty>";
	if (k[0] === BINARY_TAG) return `hex:${k.toHex()}`;
	try {
		// fatal:true throws on invalid UTF-8 so we fall back to hex rather than emitting U+FFFD garbage.
		return JSON.stringify(new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(k));
	} catch {
		return `hex:${k.toHex()}`;
	}
}

/**
 * Stable, collision-free string identity for a single key, for use as a Map/Set key (a raw
 * `Uint8Array` can't be a Map key — it compares by reference). Hex of the canonical bytes: distinct
 * keys ⇒ distinct strings. Display only via `keyForLog`; this one is for identity/lookup.
 */
function mapKey(k: KeyBytes): string {
	return k.toHex();
}

/**
 * Stable, collision-free string identity for a (hashKey, sortKey) pair — replaces the old
 * `` `${hashKey}\0${sortKey}` `` joiner, which is NOT collision-proof now that binary keys may legally
 * contain 0x00. Hex is a fixed alphabet that never contains ':', so the separator is unambiguous for
 * arbitrary bytes. Used for 2PC keyset comparison and duplicate detection.
 */
function pairKey(hk: KeyBytes, sk: KeyBytes): string {
	return `${hk.toHex()}:${sk.toHex()}`;
}

export const KeyCodec = {
	encode,
	encodeOptional,
	decode,
	compare,
	successor,
	shortestSeparator,
	asKeyBytes,
	keyForLog,
	mapKey,
	pairKey,
} as const;
