import { KeyCodec, type KeyBytes } from "../../sharding/key-codec.js";
import type { SkInterval } from "../../sharding/sk-interval.js";
import type { SortKeyCondition } from "../types.js";

/**
 * Normalize a public `SortKeyCondition` into a byte-space `SkInterval`, or `null` for an
 * unsatisfiable (empty) interval. The `encode` callback handles key encoding + size validation
 * (the interval module is agnostic to key-length policy).
 */
export function normalizeSkInterval(sort: SortKeyCondition | undefined, encode: (k: string | Uint8Array) => KeyBytes): SkInterval | null {
	if (sort === undefined) {
		return { lower: { value: KeyCodec.encodeOptional(undefined), inclusive: true } };
	}
	switch (sort.op) {
		case "eq": {
			const v = encode(sort.value);
			return { lower: { value: v, inclusive: true }, upper: { value: v, inclusive: true } };
		}
		case "gt":
			return { lower: { value: encode(sort.value), inclusive: false } };
		case "gte":
			return { lower: { value: encode(sort.value), inclusive: true } };
		case "lt":
			return { upper: { value: encode(sort.value), inclusive: false } };
		case "lte":
			return { upper: { value: encode(sort.value), inclusive: true } };
		case "between": {
			const lo = encode(sort.lower);
			const hi = encode(sort.upper);
			if (KeyCodec.compare(lo, hi) > 0) {
				return null;
			}
			return { lower: { value: lo, inclusive: true }, upper: { value: hi, inclusive: true } };
		}
		case "begins_with": {
			const prefix = sort.prefix;
			if ((typeof prefix === "string" && prefix.length === 0) || (prefix instanceof Uint8Array && prefix.byteLength === 0)) {
				return { lower: { value: KeyCodec.encodeOptional(undefined), inclusive: true } };
			}
			const p = encode(prefix);
			const succ = KeyCodec.successor(p);
			return { lower: { value: p, inclusive: true }, upper: succ ? { value: succ, inclusive: false } : undefined };
		}
		case "range": {
			const lo = sort.lower ? { value: encode(sort.lower.value), inclusive: sort.lower.inclusive } : undefined;
			const up = sort.upper ? { value: encode(sort.upper.value), inclusive: sort.upper.inclusive } : undefined;
			if (lo && up) {
				const cmp = KeyCodec.compare(lo.value, up.value);
				if (cmp > 0 || (cmp === 0 && (!lo.inclusive || !up.inclusive))) {
					return null;
				}
			}
			return { lower: lo, upper: up };
		}
	}
}
