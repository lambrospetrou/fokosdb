/**
 * The translation of a `sortKeyCondition` on a GSI sort key into an interval over the physical GSI sort
 * key. `E(k)` is the start of the entries of the GSI sort key `k`: `0xFF escape(k) 00 00`. `succ(E(k))`
 * is the end: `0xFF escape(k) 00 01`. Every entry of `k` sorts in `[E(k), succ(E(k)))`.
 */
import { KeyCodec, type KeyBytes } from "../../sharding/key-codec.js";
import type { SkInterval } from "../../sharding/sk-interval.js";
import type { SortKeyCondition } from "../types.js";
import { escapeKey } from "./sort-key.js";

function withSuffix(key: KeyBytes, last: number): KeyBytes {
	const escaped = escapeKey(key);
	const out = new Uint8Array(escaped.length + 2);
	out.set(escaped);
	out[escaped.length + 1] = last;
	return KeyCodec.encode(out);
}

const start = (key: KeyBytes) => withSuffix(key, 0x00);
const end = (key: KeyBytes) => withSuffix(key, 0x01);

/** The physical interval of a GSI query, or null when no key matches. `encode` validates and encodes each bound. */
export function gsiSkInterval(sort: SortKeyCondition | undefined, encode: (k: string | Uint8Array) => KeyBytes): SkInterval | null {
	if (sort === undefined) {
		return { lower: { value: KeyCodec.encodeOptional(undefined), inclusive: true } };
	}
	switch (sort.op) {
		case "eq": {
			const k = encode(sort.value);
			return { lower: { value: start(k), inclusive: true }, upper: { value: end(k), inclusive: false } };
		}
		case "gte":
			return { lower: { value: start(encode(sort.value)), inclusive: true } };
		case "gt":
			return { lower: { value: end(encode(sort.value)), inclusive: true } };
		case "lte":
			return { upper: { value: end(encode(sort.value)), inclusive: false } };
		case "lt":
			return { upper: { value: start(encode(sort.value)), inclusive: false } };
		case "between": {
			const lo = encode(sort.lower);
			const hi = encode(sort.upper);
			if (KeyCodec.compare(lo, hi) > 0) {
				return null;
			}
			return { lower: { value: start(lo), inclusive: true }, upper: { value: end(hi), inclusive: false } };
		}
		case "begins_with": {
			const prefix = sort.prefix;
			if ((typeof prefix === "string" && prefix.length === 0) || (prefix instanceof Uint8Array && prefix.byteLength === 0)) {
				return { lower: { value: KeyCodec.encodeOptional(undefined), inclusive: true } };
			}
			const p = encode(prefix);
			const succ = KeyCodec.successor(p);
			return {
				lower: { value: KeyCodec.encode(escapeKey(p)), inclusive: true },
				upper: succ ? { value: KeyCodec.encode(escapeKey(succ)), inclusive: false } : undefined,
			};
		}
		case "range": {
			const lo = sort.lower && encode(sort.lower.value);
			const hi = sort.upper && encode(sort.upper.value);
			if (lo && hi) {
				const cmp = KeyCodec.compare(lo, hi);
				if (cmp > 0 || (cmp === 0 && (!sort.lower!.inclusive || !sort.upper!.inclusive))) {
					return null;
				}
			}
			return {
				...(lo ? { lower: { value: sort.lower!.inclusive ? start(lo) : end(lo), inclusive: true } } : {}),
				...(hi ? { upper: { value: sort.upper!.inclusive ? end(hi) : start(hi), inclusive: false } } : {}),
			};
		}
	}
}
