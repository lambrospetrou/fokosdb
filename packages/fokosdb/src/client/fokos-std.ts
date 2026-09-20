import { KeyCodec } from "../sharding/key-codec.js";
import { validateKeyContent } from "../shared/transaction-limits.js";
import type { HashKey, QueryItemsOptions, SortKey } from "../shared/types.js";

type QuerySpec = QueryItemsOptions["queries"][number];

/**
 * Pure client-side helpers that build requests for `FokosDB`. They send no RPC of their own.
 */
export class FokosStd {
	/**
	 * The least sort key that sorts after every sort key that begins with `prefix`, in the same key
	 * type as `prefix`, so the result is always a valid query bound. `KeyCodec.publicSuccessor` holds
	 * the rules; the key-content validation of a sort-key bound applies to `prefix` here.
	 *
	 * Returns `undefined` when no such key exists: an empty prefix (every key begins with it), a string
	 * of only U+10FFFF, or bytes of only 0xFF. A query for the keys after such a prefix has no upper bound.
	 */
	static sortKeySuccessor<K extends SortKey>(prefix: K): K | undefined {
		validateKeyContent("sortKey", prefix);
		return KeyCodec.publicSuccessor(prefix) as K | undefined;
	}

	/**
	 * The `queryItems` sub-queries that select every item of `hashKey` whose sort key does not begin
	 * with `prefix`. The complement of a prefix is two sort-key ranges: the keys below the prefix, and
	 * the keys from `sortKeySuccessor(prefix)` up. Each range is one sub-query, in scan order, so the
	 * items of the result stay sorted.
	 *
	 * An item with no sort key and a binary sort key under a string prefix are both in the result: the
	 * absent sort key sorts below every prefix, and binary keys sort above every string.
	 *
	 * An empty prefix matches every sort key, so its complement is empty and the result is `[]`.
	 * `queryItems` rejects an empty `queries` list; add the result to other sub-queries or check its length.
	 */
	static notBeginsWith(hashKey: HashKey, sortKeyPrefix: SortKey, options?: { scanIndexForward?: boolean }): QuerySpec[] {
		if (sortKeyPrefix.length === 0) {
			return [];
		}
		const scanIndexForward = options?.scanIndexForward ?? true;
		const below: QuerySpec = { hashKey, sortKeyCondition: { op: "lt", value: sortKeyPrefix }, scanIndexForward };
		const successor = FokosStd.sortKeySuccessor(sortKeyPrefix);
		if (successor === undefined) {
			return [below];
		}
		const above: QuerySpec = { hashKey, sortKeyCondition: { op: "gte", value: successor }, scanIndexForward };
		return scanIndexForward ? [below, above] : [above, below];
	}
}
