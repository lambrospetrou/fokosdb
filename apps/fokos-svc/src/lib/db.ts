import { env } from "cloudflare:workers";
import { StaticShardedDO } from "durable-utils/do-sharding";
import { tryWhile } from "durable-utils/retries";
import { isErrorRetryable } from "durable-utils/do-utils";
import {
	DataKind,
	DeleteItemOptions,
	DeleteItemResult,
	EncodedItemData,
	GetItemOptions,
	GetItemResult,
	InitiateReadResponse,
	InitiateWriteResponse,
	JsonComposite,
	JsonValue,
	PutItemOptions,
	PutItemResult,
	QueryItemsMeta,
	QueryItemsOptions,
	QueryItemsResult,
} from "./types.js";
import { PartitionDO, isSinglePartitionFastPathFallbackError } from "./do-partition.js";
import { isDestroyAbortError } from "./cf-utils.js";
import { TransactionCoordinatorDO } from "./do-transaction-coordinator.js";
import type { PartitionTopologyRouter } from "./partition-topology/router.js";
import type {
	InitiateReadResponseEncoded,
	TCWriteOperation,
	TCReadItem,
	TransactGetItemsOptions,
	TransactWriteItemsOptions,
} from "./transaction-types.js";
import {
	encodeHashKey,
	encodeSortBound,
	encodeSortKey,
	validateItemDataSize,
	validateItemKeys,
	singlePartitionTarget,
	validateTransactGetItemCount,
	validateTransactWriteOperations,
} from "./transaction-limits.js";
import { KeyCodec } from "./partition-topology/key-codec.js";
import type { PartitionInfoInternal } from "./partition-topology/types.js";
import { normalizeSkInterval } from "./query/sk-interval.js";
import type { ScanCursor } from "./partition/partition-store.js";
import { CURSOR_VERSION, encodeCursor, decodeCursor, computeCursorFingerprint, type DecodedCursor } from "./query/cursor.js";
import { PageBudget } from "./query/page-budget.js";

export const DEFAULT_NUM_TRANSACTION_COORDINATORS = 100;

// The single JS↔wire encode boundary for item data: a Uint8Array is opaque bytes,
// a string is opaque text, and an object/array is JSON — stringified exactly once here
// so the DO only ever receives `string | Uint8Array` plus a kind discriminant.
function encodeItemData(data: string | Uint8Array | JsonComposite): EncodedItemData {
	if (data instanceof Uint8Array) return { kind: "bytes", data };
	if (typeof data === "string") return { kind: "text", data };
	// `JsonComposite` is arrays and objects only.
	// Accepting a primitive silently would make the declared type a lie, and taking it back later would be
	// breaking — whereas relaxing this check later is not.
	if (data === null || typeof data !== "object") {
		throw new Error(`fokos: data must be an object, array, string or Uint8Array (got ${data === null ? "null" : typeof data})`);
	}
	let text: string;
	try {
		text = JSON.stringify(data);
	} catch (err) {
		// A circular reference or a BigInt. Only JSON.stringify knows which, so quote it.
		throw new Error(`fokos: data is not JSON-serializable (${String(err)})`, { cause: err });
	}
	// The guard above rules out every value that JSON.stringify drops, with one exception: a `toJSON`
	// that itself returns undefined (or a function, or a symbol) makes the WHOLE document undefined.
	if (text === undefined) {
		throw new Error("fokos: data is not JSON-serializable (its toJSON() returned undefined)");
	}
	return { kind: "json", data: text };
}

// The matching decode boundary: json rows arrive from the DO as JSON text, parsed once back to a
// JsonValue; bytes/text pass through untouched. A parse failure means the stored JSONB → json() text
// is malformed (a store/encoding bug, not user input), so surface it loudly rather than returning junk.
function decodeItemData(kind: DataKind, data: string | Uint8Array | JsonValue): string | Uint8Array | JsonValue {
	if (kind !== "json") return data;
	try {
		return JSON.parse(data as string);
	} catch (err) {
		console.error({
			message: "fokos: failed to parse json item data returned by the store",
			error: String(err),
			errorProps: err,
		});
		throw new Error("fokos: failed to parse json item data returned by the store", { cause: err });
	}
}

export type FokosDBOptions = {
	topology: PartitionTopologyRouter;
	transactionCoordinatorNs: DurableObjectNamespace<TransactionCoordinatorDO>;

	// TODO Temporary since ideally the transaction coordinators should also auto-scale.
	// This is safe to increase if needed except for retrying the same transaction with the same idempotency token.
	// Data partitions record the actual DO name they should reach out for recovering the transaction.
	numTransactionCoordinators?: number;

	/**
	 * Runs a transaction whose items are all owned by ONE partition against that partition directly,
	 * in a single round trip, instead of through a transaction coordinator. Defaults to true.
	 *
	 * It is an execution strategy, not a semantic: both paths give the same answer, so it belongs
	 * here and not on the per-call options. Set it to false to force every transaction through the
	 * coordinator.
	 */
	singlePartitionFastPath?: boolean;
};

/**
 * Drops `_internal` from a partition meta. This is where partition-to-partition routing state stops:
 * every DO response carries the serving leaf's `rangeAncestors` so routers can cache them, and none of
 * that is meaningful to a client.
 * Public results are typed `PartitionInfo`, which has no such field, but structural typing accepts an
 * object that carries extra properties, so the removal has to happen at runtime as well.
 */
function publicMeta<T extends PartitionInfoInternal>(meta: T): Omit<T, "_internal"> {
	const { _internal: _dropped, ...rest } = meta;
	return rest;
}

export class FokosDB {
	#options: Required<FokosDBOptions>;
	#staticShardedTCs: StaticShardedDO<TransactionCoordinatorDO>;

	constructor(options: FokosDBOptions) {
		this.#options = {
			...options,
			numTransactionCoordinators: options.numTransactionCoordinators ?? DEFAULT_NUM_TRANSACTION_COORDINATORS,
			singlePartitionFastPath: options.singlePartitionFastPath ?? true,
		};
		if (!Number.isInteger(this.#options.numTransactionCoordinators) || this.#options.numTransactionCoordinators <= 0) {
			throw new Error("fokosdb: numTransactionCoordinators must be an integer greater or equal to 1");
		}
		this.#staticShardedTCs = new StaticShardedDO(this.#options.transactionCoordinatorNs, {
			numShards: this.#options.numTransactionCoordinators,
			shardGroupName: `${this.#options.topology.partitionContext().tableName}.tc`,
		});
	}

	options() {
		return { ...this.#options };
	}

	async putItem(opts: PutItemOptions): Promise<PutItemResult> {
		if (opts.ttlEpochUTCSeconds !== undefined || opts.ttlSeconds !== undefined) {
			throw new Error("fokosdb: TTL expiration not yet implemented");
		}
		validateItemKeys(opts.hashKey, opts.sortKey);
		const hashKey = encodeHashKey(opts.hashKey);
		const sortKey = encodeSortKey(opts.sortKey);
		const { doId, partitionContext } = this.#options.topology.pickPartition(hashKey, sortKey);
		const stub = PartitionDO.get(env[this.#options.topology.partitionContext().ns], doId);
		// Encode data once at this boundary; the DO receives string | Uint8Array + kind.
		const encoded = encodeItemData(opts.data);
		// Measured on the ENCODED form, so a json payload is capped by the text actually stored and
		// the same item is accepted or rejected identically here and in transactWriteItems.
		validateItemDataSize(encoded.data, "putItem");
		// ttlSeconds is deliberately not forwarded: no layer honours it yet.
		const res = await stub.apiPutItem(partitionContext, {
			hashKey,
			sortKey,
			data: encoded.data,
			kind: encoded.kind,
			ttlEpochUTCSeconds: opts.ttlEpochUTCSeconds,
			conditions: opts.conditions,
		});
		// The DO returns no keys; the caller's own are the only ones it can recognise.
		return { item: { hashKey: opts.hashKey, sortKey: opts.sortKey }, version: res.version, meta: publicMeta(res.meta) };
	}

	async getItem(opts: GetItemOptions): Promise<GetItemResult> {
		validateItemKeys(opts.hashKey, opts.sortKey);
		const hashKey = encodeHashKey(opts.hashKey);
		const sortKey = encodeSortKey(opts.sortKey);
		const { doId, partitionContext } = this.#options.topology.pickPartition(hashKey, sortKey);
		const stub = PartitionDO.get(env[this.#options.topology.partitionContext().ns], doId);
		const res = await stub.apiGetItem(partitionContext, { hashKey, sortKey });
		// The DO returns no keys; supply the caller's own and preserve the found/not-found discriminant.
		// json data arrives as JSON text — parse it once here to the public JsonValue.
		if (res.found) {
			return {
				found: true,
				item: { ...res.item, hashKey: opts.hashKey, sortKey: opts.sortKey, data: decodeItemData(res.item.kind, res.item.data) },
				meta: publicMeta(res.meta),
			};
		}
		return { found: false, item: { hashKey: opts.hashKey, sortKey: opts.sortKey }, meta: publicMeta(res.meta) };
	}

	async deleteItem(opts: DeleteItemOptions): Promise<DeleteItemResult> {
		validateItemKeys(opts.hashKey, opts.sortKey);
		const hashKey = encodeHashKey(opts.hashKey);
		const sortKey = encodeSortKey(opts.sortKey);
		const { doId, partitionContext } = this.#options.topology.pickPartition(hashKey, sortKey);
		const stub = PartitionDO.get(env[this.#options.topology.partitionContext().ns], doId);
		const res = await stub.apiDeleteItem(partitionContext, { hashKey, sortKey, conditions: opts.conditions });
		// The DO returns no keys; the caller's own are the only ones it can recognise.
		return { item: { hashKey: opts.hashKey, sortKey: opts.sortKey }, deleted: res.deleted, meta: publicMeta(res.meta) };
	}

	async transactWriteItems(opts: TransactWriteItemsOptions): Promise<InitiateWriteResponse> {
		// Encode a put's data once at this boundary; the TC/DO see string | Uint8Array + kind. Validation
		// below then measures the encoded form. A non-put is passed through untouched, so a `data` field
		// set by a non-TypeScript caller still reaches validation.
		const prepared = opts.items.map((item) => (item.operation === "put" ? { ...item, ...encodeItemData(item.data) } : item));
		// Validation encodes each key exactly once and hands the canonical bytes back in input order.
		const keys = validateTransactWriteOperations(prepared);
		const items: TCWriteOperation[] = prepared.map((item, i) => {
			const { hashKey, sortKey } = keys[i];
			const { partitionContext } = this.#options.topology.pickPartition(hashKey, sortKey);
			return { ...item, hashKey, sortKey, partitionContext };
		});

		// TODO: We need to catch DO errors and retry with a different idempotency token to route
		// to a different TC if the chosen one is overloaded or has failed. Tricky to do for writes though...
		const idempotencyToken = opts.clientRequestToken ?? crypto.randomUUID().replaceAll("-", "");
		// The TC response carries no keys — nothing to decode at this boundary, unlike every other
		// method here. See InitiateWriteResponse.
		return await this.#staticShardedTCs.one(idempotencyToken, async (tcStub: DurableObjectStub<TransactionCoordinatorDO>) => {
			return await tcStub.initiateWrite({ clientRequestToken: idempotencyToken, items });
		});
	}

	async transactGetItems(opts: TransactGetItemsOptions): Promise<InitiateReadResponse> {
		validateTransactGetItemCount(opts.items.length);
		const items: TCReadItem[] = opts.items.map((item) => {
			validateItemKeys(item.hashKey, item.sortKey);
			const hashKey = encodeHashKey(item.hashKey);
			const sortKey = encodeSortKey(item.sortKey);
			const { partitionContext } = this.#options.topology.pickPartition(hashKey, sortKey);
			return { ...item, hashKey, sortKey, partitionContext };
		});

		const response = (await this.#readSnapshotFastPath(items)) ?? (await this.#readViaCoordinator(items));

		// The public boundary — the single exit where the internal representation becomes the public one:
		// decode the KeyBytes back to public keys (the empty sentinel maps to an absent sortKey, same as
		// queryItems), parse json text once into a JsonValue, and drop the TC-only 2PC bookkeeping
		// (lastCommittedTs / hasPendingWrite) so callers never depend on it. Those two are meaningless in
		// a "committed" outcome regardless — the TC aborts when any item has a pending write.
		if (response.outcome !== "committed") return response;
		return {
			...response,
			items: response.items.map(({ lastCommittedTs: _lastCommittedTs, hasPendingWrite: _hasPendingWrite, hashKey, sortKey, ...item }) => {
				const keys = {
					hashKey: KeyCodec.decode(hashKey),
					sortKey: sortKey.byteLength === 0 ? undefined : KeyCodec.decode(sortKey),
				};
				return item.found ? { ...item, ...keys, data: decodeItemData(item.kind, item.data) } : { ...item, ...keys };
			}),
		};
	}

	/**
	 * One round trip to the owning partition when every requested key resolves to it. Returns null
	 * when the fast path does not apply, so the caller runs the coordinator path: either the client
	 * hint says the keys span partitions, or the partition itself answered that they do.
	 */
	async #readSnapshotFastPath(items: TCReadItem[]): Promise<InitiateReadResponseEncoded | null> {
		if (!this.#options.singlePartitionFastPath) return null;
		const target = singlePartitionTarget(items);
		if (!target) return null;

		const stub = PartitionDO.getByName(env[target.ns], target.doName);
		const request = { items: items.map(({ hashKey, sortKey }) => ({ hashKey, sortKey })) };
		try {
			return await tryWhile(
				async () => await stub.txReadSnapshot(target, request),
				(err: unknown, nextAttempt: number) => isErrorRetryable(err) && nextAttempt <= 3,
			);
		} catch (err) {
			// The fallback is the ONE error that means "run the coordinator path instead". It carries no
			// side effects, so nothing was read and nothing has to be undone. Every other error — a
			// transport failure included — is the caller's, exactly as on the coordinator path.
			if (!isSinglePartitionFastPathFallbackError(err)) throw err;
			return null;
		}
	}

	async #readViaCoordinator(items: TCReadItem[]): Promise<InitiateReadResponseEncoded> {
		return await tryWhile(
			async () => {
				// Read-only TCs are ephemeral — random UUID, no client idempotency token needed.
				// Even better using a different shard key each time to maximize chances of hitting different TCs if there is an overloaded one.
				return await this.#staticShardedTCs.one(crypto.randomUUID(), async (tcStub: DurableObjectStub<TransactionCoordinatorDO>) => {
					return await tcStub.initiateRead({ items });
				});
			},
			(err: unknown, nextAttempt: number) => isErrorRetryable(err) && nextAttempt <= 3,
		);
	}

	async queryItems(opts: QueryItemsOptions): Promise<QueryItemsResult> {
		if (opts.queries.length === 0) {
			throw new Error("fokos/queryItems: queries must not be empty");
		}
		if (opts.limit !== undefined && (!Number.isSafeInteger(opts.limit) || opts.limit <= 0)) {
			throw new Error("fokos/queryItems: limit must be a positive integer when provided");
		}
		if (opts.maxPageBytes !== undefined && (!Number.isSafeInteger(opts.maxPageBytes) || opts.maxPageBytes <= 0)) {
			throw new Error("fokos/queryItems: maxPageBytes must be a positive integer when provided");
		}

		const normalizedQueries = opts.queries.map((q) => {
			const direction = (q.scanIndexForward ?? true) ? ("asc" as const) : ("desc" as const);
			// A query hash key is a whole item key and gets the full rules, so a key that cannot be
			// written cannot be queried either. Sort-key BOUNDS get only the content rules: they are not
			// item keys, and `begins_with: ""` is a legitimate "everything" query.
			validateItemKeys(q.hashKey);
			return {
				hashKey: encodeHashKey(q.hashKey),
				interval: normalizeSkInterval(q.sortKeyCondition, encodeSortBound),
				direction,
				cursorDirection: direction === "asc" ? ("fwd" as const) : ("rev" as const),
			};
		});
		const fingerprint = computeCursorFingerprint(normalizedQueries);

		const DEFAULT_MAX_PAGE_BYTES = 3 * 1024 * 1024;
		const SERVER_MAX_PAGE_BYTES = 16 * 1024 * 1024;
		const budget = new PageBudget(Math.min(opts.maxPageBytes ?? DEFAULT_MAX_PAGE_BYTES, SERVER_MAX_PAGE_BYTES), opts.limit ?? null, 100);

		let startQueryIdx = 0;
		let startInner: DecodedCursor["inner"] = null;
		if (opts.cursor !== undefined) {
			const decoded = decodeCursor(opts.cursor);
			if (decoded.queryIdx >= normalizedQueries.length) throw new Error("fokos/queryItems: cursor queryIdx out of range");
			if (decoded.direction !== normalizedQueries[decoded.queryIdx].cursorDirection)
				throw new Error("fokos/queryItems: cursor direction mismatch — scanIndexForward differs from the page that issued this cursor");
			if (decoded.fingerprint !== fingerprint) throw new Error("fokos/queryItems: cursor fingerprint mismatch — re-send the same request");
			startQueryIdx = decoded.queryIdx;
			startInner = decoded.inner;
		}

		const items: QueryItemsResult["items"] = [];
		const partitionMetas: QueryItemsResult["partitionMetas"] = [];
		let forwardCount = 0;
		let cursor: string | undefined;

		for (let qi = startQueryIdx; qi < normalizedQueries.length; qi++) {
			const query = normalizedQueries[qi];
			if (query.interval === null) continue;

			const rpcCursor: ScanCursor | null =
				qi === startQueryIdx && startInner !== null
					? { hk: startInner.hashKey, sk: startInner.sortKey, inclusive: startInner.inclusive }
					: null;

			const { doId, partitionContext } = this.#options.topology.pickPartition(query.hashKey, KeyCodec.encodeOptional(undefined));
			const stub = PartitionDO.get(env[this.#options.topology.partitionContext().ns], doId);

			const rpcResult = await stub.apiQueryItems(partitionContext, {
				hashKey: query.hashKey,
				interval: query.interval,
				direction: query.direction,
				budgetBytes: budget.remainingBytes,
				remainingLimit: budget.remainingLimit,
				maxPartitionVisits: budget.remainingVisits,
				cursor: rpcCursor,
			});

			for (const item of rpcResult.items) {
				items.push({
					hashKey: KeyCodec.decode(item.hk),
					sortKey: item.sk.byteLength === 0 ? undefined : KeyCodec.decode(item.sk),
					// json data arrives as JSON text — parse it once here to the public JsonValue.
					data: decodeItemData(item.kind, item.data),
					kind: item.kind,
					ttlEpochUTCSeconds: item.ttl_epoch_utc_seconds ?? undefined,
					version: item.v,
				});
			}
			partitionMetas.push(...rpcResult.partitionMetas.map(publicMeta));
			forwardCount += rpcResult.meta.forwardCount;
			budget.consume(rpcResult.bytesConsumed, rpcResult.items.length, rpcResult.partitionMetas.length);

			if (rpcResult.nextCursor !== null) {
				cursor = encodeCursor({
					version: CURSOR_VERSION,
					direction: query.cursorDirection,
					fingerprint,
					queryIdx: qi,
					inner: {
						hashKey: rpcResult.nextCursor.hk,
						sortKey: rpcResult.nextCursor.sk,
						inclusive: rpcResult.nextCursor.inclusive ?? false,
					},
				});
				break;
			}

			if (budget.exhausted) {
				if (budget.visitsExhausted) {
					console.warn("fokos/queryItems: maxPartitionVisits budget exhausted across sub-queries, paginating early");
				}
				let nextQueryIdx = -1;
				for (let j = qi + 1; j < normalizedQueries.length; j++) {
					if (normalizedQueries[j].interval !== null) {
						nextQueryIdx = j;
						break;
					}
				}
				if (nextQueryIdx !== -1) {
					cursor = encodeCursor({
						version: CURSOR_VERSION,
						direction: normalizedQueries[nextQueryIdx].cursorDirection,
						fingerprint,
						queryIdx: nextQueryIdx,
						inner: null,
					});
				}
				break;
			}
		}

		const meta: QueryItemsMeta = {
			rowsRead: partitionMetas.reduce((s, m) => s + m.rowsRead, 0),
			rowsReturned: items.length,
			forwardCount,
			partitionsVisited: partitionMetas.length,
		};

		return { items, count: items.length, cursor, meta, partitionMetas };
	}

	async destroy(): Promise<{ ok: true }> {
		const ns = this.#options.topology.partitionContext().ns;

		// Coordinators first, partitions second. A transaction still in flight is driven BY a coordinator,
		// so wiping the coordinators stops the drivers before the data goes; the reverse order lets a live
		// coordinator commit into a partition that was just emptied and leave rows behind the traversal has
		// already passed. Every shard is swept, not only the ones that hold rows: the shard for a given
		// idempotency token is not knowable from here, and a shard with no rows costs one wipe of empty
		// storage.
		await this.#staticShardedTCs.all(async (tcStub: DurableObjectStub<TransactionCoordinatorDO>, shard: number) => {
			try {
				await tcStub.destroyCoordinator();
			} catch (e) {
				// destroyCoordinator ends in ctx.abort(), which always surfaces here as a throw.
				if (!isDestroyAbortError(e)) throw e;
			}
			console.warn(`Destroyed transaction coordinator shard ${shard}`);
		});

		// The router owns the traversal (child-discovery order, range-root resolution, dedup);
		// FokosDB supplies the two callbacks that perform the RPCs.
		await this.#options.topology.traverseForDestroy(
			async (ctx) => {
				const stub = PartitionDO.getByName(env[ns], ctx.doName);
				console.warn(`Destroying partition DO ${ctx.doName} (partitionId=${ctx.partitionId})`);
				const { splitStatus, promotedKeys } = await stub.status(ctx);
				return { splitStatus, promotedKeys };
			},
			async (ctx) => {
				const stub = PartitionDO.getByName(env[ns], ctx.doName);
				try {
					await stub.destroyPartition();
				} catch (e) {
					// console.error(`Error destroying partition DO ${ctx.doName} (partitionId=${ctx.partitionId}):`, e);
					if (!isDestroyAbortError(e)) throw e;
				}
				console.warn(`Destroyed partition DO ${ctx.doName} (partitionId=${ctx.partitionId})`);
			},
		);

		return { ok: true };
	}
}
