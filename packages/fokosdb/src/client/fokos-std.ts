import { systemPlatform, type FokosPlatform } from "../shared/platform.js";
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
	private constructor() {}

	/**
	 * Calls `db.batchGetItems` with every key, then again with `unprocessedKeys` and the `projection` and
	 * `maxResponseBytes` of the request, until no key stays unprocessed or the calls with an error reach
	 * `retry.maxAttempts`. Before each retry after an error it waits a random time with exponential
	 * backoff (`retry.baseDelayMs`, `retry.maxDelayMs`). The random values and the wait come from
	 * `db.platform`. When `retry` is absent, it uses `db.retry`.
	 *
	 * A key that is `unprocessed` with `response_too_large` is on a later page. It is not an error: a
	 * later call sends it with no backoff wait, and that call does not count as an attempt. `batchGetItems` always
	 * answers the first key of a call that has no error, so the pages end. After such a page, the next
	 * call sends at most two times the keys that the page answered, in request order, because each
	 * partition reads up to the limit, and keys that cannot fit in the page are read for nothing. A key
	 * that waits keeps its last answer, `response_too_large`, when the attempts end.
	 *
	 * The result has the positions of the first request: `items[i]` is the last answer for `opts.items[i]`.
	 * `meta` joins the `meta` of every call. A key that is still unprocessed after the last call stays in
	 * `unprocessedKeys`. It takes the limits of `batchGetItems`, 0 to 1,000 keys, and makes one call at a
	 * time. It throws only for options that are not valid, before any I/O.
	 *
	 * `maxResponseBytes` limits each call, not the result. The result holds the items of all pages in
	 * memory together.
	 */
	static async batchGetItemsWithRetry<Ts extends readonly unknown[] = never[]>(
		db: ItemBatcher,
		opts: NoInfer<BatchGetItemsOptions<Ts>>,
		retry?: FokosDBRetryOptions,
	): Promise<BatchGetItemsResult<Ts>> {
		const policy = resolveRetryOptions(retry ?? db.retry);
		const { projection, maxResponseBytes } = opts as BatchGetItemsOptions;
		const { results, unprocessed, meta } = await retryBatch<BatchGetItemKey, BatchGetItemResult>(
			policy,
			db.platform ?? systemPlatform,
			opts.items as readonly BatchGetItemKey[],
			async (items) => {
				const answer = await db.batchGetItems({ items, projection, maxResponseBytes });
				return { results: answer.items, meta: answer.meta };
			},
			(result) => result.outcome === "unprocessed",
			(_earlier, later) => later,
			(result) => result.outcome === "unprocessed" && result.code === "response_too_large",
		);
		// Every item takes the same path, so the positional types of `Ts` are erased here, as in `db.ts`.
		return { items: results, unprocessedKeys: unprocessed, meta } as unknown as BatchGetItemsResult<Ts>;
	}

	/**
	 * Calls `db.batchWriteItems` with every operation, then again with `unprocessedItems`, as
	 * `batchGetItemsWithRetry` does. It never sends a `rejected` operation again. An operation that got
	 * `unknown` keeps `unknown` when a later call does not apply it, because the earlier call can have
	 * applied it. `results[i]` answers `opts.items[i]`. A write has no pages, so each call counts as an
	 * attempt.
	 */
	static async batchWriteItemsWithRetry(
		db: ItemBatcher,
		opts: BatchWriteItemsOptions,
		retry?: FokosDBRetryOptions,
	): Promise<BatchWriteItemsResult> {
		const policy = resolveRetryOptions(retry ?? db.retry);
		const { results, unprocessed, meta } = await retryBatch<BatchWriteItem, BatchWriteItemResult>(
			policy,
			db.platform ?? systemPlatform,
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
 *
 * `paged` selects the results to send again that are on the next page and are not an error. After a
 * call with no error, the next call sends at most two times the entries that the call answered. A call
 * after an error sends as many entries as the call before it. The entries that wait stay after the
 * entries of the call, so each call holds the first entries in request order. The first call sends
 * every entry, so each entry has a result.
 *
 * A call counts as an attempt when a result to send again is not `paged`, or when the call answers no
 * entry. Only such a call waits before the next call. Any other call answers at least one entry, so
 * the loop ends.
 */
async function retryBatch<Entry, Result extends { partitionIndex?: number }>(
	retry: Required<FokosDBRetryOptions>,
	platform: FokosPlatform,
	entries: readonly Entry[],
	call: (entries: Entry[]) => Promise<{ results: Result[]; meta: FokosMeta }>,
	again: (result: Result) => boolean,
	keep: (earlier: Result, later: Result) => Result,
	paged: (result: Result) => boolean = () => false,
): Promise<{ results: Result[]; unprocessed: Entry[]; meta: FokosMeta }> {
	const join = new PartitionMetaJoin();
	let forwardCount = 0;
	const results: Result[] = [];
	// The positions in `entries` that have no final answer yet, in request order.
	let pending = entries.map((_, i) => i);
	// The most entries that the next call sends. Each partition of a batch read reads its keys up to
	// the response limit, and the client then keeps only one limit of items for all partitions together.
	// So when a call sends all the remaining keys to many partitions, the partitions read many items that
	// the client drops, and the next page reads them again. Example: 1,000 keys of 1 MiB on 64
	// partitions, with a limit of 24 MiB, read about 21,000 items for 1,000 answers. When each call
	// sends at most two times the keys that the call before it answered, they read about 3,000 items.
	let sendCount = pending.length;
	let failedCalls = 0;
	// The first call always runs, also with no entry, so the method validates the options.
	for (;;) {
		const sent = pending.slice(0, sendCount);
		const answer = await call(sent.map((position) => entries[position]));
		forwardCount += answer.meta.forwardCount;
		const indexes = answer.meta.partitions.map((entry) => join.add(entry));
		const next: number[] = [];
		let failed = false;
		answer.results.forEach((result, j) => {
			const moved = result.partitionIndex === undefined ? result : { ...result, partitionIndex: indexes[result.partitionIndex] };
			const position = sent[j];
			results[position] = results[position] === undefined ? moved : keep(results[position], moved);
			if (again(result)) {
				next.push(position);
				failed ||= !paged(result);
			}
		});
		// The keys that the call did not answer go before the keys that it did not send. So the next call
		// sends the first keys in request order, and the first key of a call is always answered.
		pending = next.concat(pending.slice(sent.length));
		if (pending.length === 0) {
			break;
		}
		if (failed || next.length === sent.length) {
			failedCalls++;
			if (failedCalls >= retry.maxAttempts) {
				break;
			}
			// The first retry is retry 0 of the backoff.
			await platform.wait(jitterBackoff(failedCalls - 1, retry.baseDelayMs, retry.maxDelayMs, platform));
		} else {
			// A page with no error shows how many keys fit in one response. Two times that number lets the
			// window grow when the next keys are smaller, and limits the reads when they are not. After an
			// error the window keeps its size, because a fault does not show how many keys fit.
			sendCount = 2 * (sent.length - next.length);
		}
	}
	return {
		results,
		unprocessed: entries.filter((_, i) => again(results[i])),
		meta: { forwardCount, partitions: join.partitions },
	};
}
