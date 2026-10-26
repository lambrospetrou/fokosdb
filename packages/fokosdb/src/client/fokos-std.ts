import { jitterBackoff } from "durable-utils/retries";
import { KeyCodec } from "../sharding/key-codec.js";
import type {
	BatchGetItemKey,
	BatchGetItemResult,
	BatchGetItemsOptions,
	BatchGetItemsResult,
	BatchWriteItem,
	BatchWriteItemResult,
	BatchWriteItemsOptions,
	BatchWriteItemsResult,
	ItemBatcher,
} from "../shared/batch-api-types.js";
import { PartitionMetaJoin } from "../shared/partition-meta.js";
import { validateKeyContent } from "../shared/transaction-limits.js";
import type { FokosMeta, HashKey, QueryItemsOptions, SortKey } from "../shared/types.js";
import { resolveRetryOptions, type FokosDBRetryOptions } from "./db.js";

type QuerySpec = QueryItemsOptions["queries"][number];

/**
 * Client-side helpers for `FokosDB`. The request builders send no RPC. The retry helpers send requests
 * only through the `ItemBatcher` they get.
 */
export class FokosDBStd {
	/**
	 * Calls `db.batchGetItems` with every key, then again with `unprocessedKeys` and the `projection` and
	 * `maxResponseBytes` of the request, until no key stays unprocessed or the calls reach
	 * `retry.maxAttempts`. Before each retry it waits a random time with exponential backoff
	 * (`retry.baseDelayMs`, `retry.maxDelayMs`).
	 *
	 * The result has the positions of the first request: `items[i]` is the last answer for `opts.items[i]`.
	 * `meta` joins the `meta` of every call. A key that is still unprocessed after the last call stays in
	 * `unprocessedKeys`. It takes the limits of `batchGetItems`, 0 to 100 keys, and makes one call at a
	 * time. It throws only for options that are not valid, before any I/O.
	 */
	static async batchGetItemsWithRetry<Ts extends readonly unknown[] = never[]>(
		db: ItemBatcher,
		opts: NoInfer<BatchGetItemsOptions<Ts>>,
		retry?: FokosDBRetryOptions,
	): Promise<BatchGetItemsResult<Ts>> {
		const policy = resolveRetryOptions(retry);
		const { projection, maxResponseBytes } = opts as BatchGetItemsOptions;
		const { results, unprocessed, meta } = await retryBatch<BatchGetItemKey, BatchGetItemResult>(
			policy,
			opts.items as readonly BatchGetItemKey[],
			async (items) => {
				const answer = await db.batchGetItems({ items, projection, maxResponseBytes });
				return { results: answer.items, meta: answer.meta };
			},
			(result) => result.outcome === "unprocessed",
			(_earlier, later) => later,
		);
		// Every item takes the same path, so the positional types of `Ts` are erased here, as in `db.ts`.
		return { items: results, unprocessedKeys: unprocessed, meta } as unknown as BatchGetItemsResult<Ts>;
	}

	/**
	 * Calls `db.batchWriteItems` with every operation, then again with `unprocessedItems`, as
	 * `batchGetItemsWithRetry` does. It never sends a `rejected` operation again. An operation that got
	 * `unknown` keeps `unknown` when a later call does not apply it, because the earlier call can have
	 * applied it. `results[i]` answers `opts.items[i]`.
	 */
	static async batchWriteItemsWithRetry(
		db: ItemBatcher,
		opts: BatchWriteItemsOptions,
		retry?: FokosDBRetryOptions,
	): Promise<BatchWriteItemsResult> {
		const policy = resolveRetryOptions(retry);
		const { results, unprocessed, meta } = await retryBatch<BatchWriteItem, BatchWriteItemResult>(
			policy,
			opts.items,
			async (items) => {
				const answer = await db.batchWriteItems({ items });
				return { results: answer.results, meta: answer.meta };
			},
			(result) => result.outcome === "unprocessed" || result.outcome === "unknown",
			(earlier, later) => (earlier.outcome === "unknown" && later.outcome !== "ok" ? earlier : later),
		);
		return { results, unprocessedItems: unprocessed, meta };
	}

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
		const successor = FokosDBStd.sortKeySuccessor(sortKeyPrefix);
		if (successor === undefined) {
			return [below];
		}
		const above: QuerySpec = { hashKey, sortKeyCondition: { op: "gte", value: successor }, scanIndexForward };
		return scanIndexForward ? [below, above] : [above, below];
	}
}

/**
 * The retry loop of the batch helpers. `again` selects the results to send again, and `keep` chooses
 * between the result that an item has and the result of a later call. Each result keeps the position
 * of its entry in `entries`, and each `partitionIndex` moves to the joined `meta`.
 */
async function retryBatch<Entry, Result extends { partitionIndex?: number }>(
	retry: Required<FokosDBRetryOptions>,
	entries: readonly Entry[],
	call: (entries: Entry[]) => Promise<{ results: Result[]; meta: FokosMeta }>,
	again: (result: Result) => boolean,
	keep: (earlier: Result, later: Result) => Result,
): Promise<{ results: Result[]; unprocessed: Entry[]; meta: FokosMeta }> {
	const join = new PartitionMetaJoin();
	let forwardCount = 0;
	const results: Result[] = [];
	// `positions[j]` is the position in `entries` of the entry `j` of the next call.
	let positions = entries.map((_, i) => i);
	for (let calls = 1; ; calls++) {
		const answer = await call(positions.map((position) => entries[position]));
		forwardCount += answer.meta.forwardCount;
		const indexes = answer.meta.partitions.map((entry) => join.add(entry));
		const next: number[] = [];
		answer.results.forEach((result, j) => {
			const moved = result.partitionIndex === undefined ? result : { ...result, partitionIndex: indexes[result.partitionIndex] };
			const position = positions[j];
			results[position] = results[position] === undefined ? moved : keep(results[position], moved);
			if (again(result)) {
				next.push(position);
			}
		});
		if (next.length === 0 || calls >= retry.maxAttempts) {
			break;
		}
		// The first retry is retry 0 of the backoff.
		await new Promise((resolve) => setTimeout(resolve, jitterBackoff(calls - 1, retry.baseDelayMs, retry.maxDelayMs)));
		positions = next;
	}
	return {
		results,
		unprocessed: entries.filter((_, i) => again(results[i])),
		meta: { forwardCount, partitions: join.partitions },
	};
}
