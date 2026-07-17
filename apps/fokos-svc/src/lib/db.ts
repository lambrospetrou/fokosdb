import { env } from "cloudflare:workers";
import { StaticShardedDO } from "durable-utils/do-sharding";
import { tryWhile } from "durable-utils/retries";
import { isErrorRetryable } from "durable-utils/do-utils";
import {
	DataKind,
	DeleteItemOptions,
	EncodedItemData,
	GetItemOptions,
	GetItemResult,
	InitiateReadResponse,
	InitiateWriteResponse,
	JsonComposite,
	JsonValue,
	PutItemOptions,
	QueryItemsMeta,
	QueryItemsOptions,
	QueryItemsResult,
} from "./types.js";
import { PartitionDO } from "./do-partition.js";
import { TransactionCoordinatorDO } from "./do-transaction-coordinator.js";
import type { PartitionTopologyRouter } from "./partition-topology/router.js";
import type { TCWriteOperation, TCReadItem } from "./transaction-types.js";
import { validateItemKeys, validateTransactWriteOperations } from "./transaction-limits.js";
import { KeyCodec, type KeyBytes } from "./partition-topology/key-codec.js";
import type { ItemCondition } from "./types.js";
import { normalizeSkInterval } from "./query/sk-interval.js";
import type { ScanCursor } from "./partition/partition-store.js";
import { CURSOR_VERSION, encodeCursor, decodeCursor, computeCursorFingerprint, type DecodedCursor } from "./query/cursor.js";
import { PageBudget } from "./query/page-budget.js";

export const DEFAULT_NUM_TRANSACTION_COORDINATORS = 100;

// DynamoDB-style encoded-byte ceilings. Measured on KeyBytes (after UTF-8 encoding / 0xFF tagging).
// DynamoDB uses 2KB for hashKey and 1KB for sortKey.
// We start stricter and we can raise later.
const MAX_HASH_KEY_BYTES = 1024;
const MAX_SORT_KEY_BYTES = 512;

function encodeHashKey(k: string | Uint8Array): KeyBytes {
	const bytes = KeyCodec.encode(k);
	if (bytes.byteLength > MAX_HASH_KEY_BYTES) {
		throw new Error(`fokos: hashKey exceeds ${MAX_HASH_KEY_BYTES} bytes when encoded (got ${bytes.byteLength})`);
	}
	return bytes;
}

function encodeSortKey(k: string | Uint8Array | undefined): KeyBytes {
	if (k === undefined) return KeyCodec.encodeOptional(undefined);
	const bytes = KeyCodec.encode(k);
	if (bytes.byteLength > MAX_SORT_KEY_BYTES) {
		throw new Error(`fokos: sortKey exceeds ${MAX_SORT_KEY_BYTES} bytes when encoded (got ${bytes.byteLength})`);
	}
	return bytes;
}

// The single JS↔wire encode boundary for item data: a Uint8Array is opaque bytes,
// a string is opaque text, and an object/array is JSON — stringified exactly once here
// so the DO only ever receives `string | Uint8Array` plus a kind discriminant.
function encodeItemData(data: string | Uint8Array | JsonComposite): EncodedItemData {
	if (data instanceof Uint8Array) return { kind: "bytes", data };
	if (typeof data === "string") return { kind: "text", data };
	let text: string;
	try {
		text = JSON.stringify(data);
	} catch {
		throw new Error("fokos: data is not JSON-serializable");
	}
	if (text === undefined) throw new Error("fokos: data serialized to undefined");
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
};

export class FokosDB {
	#options: Required<FokosDBOptions>;
	#staticShardedTCs: StaticShardedDO<TransactionCoordinatorDO>;

	constructor(options: FokosDBOptions) {
		this.#options = {
			...options,
			numTransactionCoordinators: options.numTransactionCoordinators ?? DEFAULT_NUM_TRANSACTION_COORDINATORS,
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

	async putItem(opts: PutItemOptions) {
		if (opts.ttlEpochUTCSeconds !== undefined && opts.ttlSeconds !== undefined) {
			throw new Error("fokosdb: TTL expiration not yet implemented");
		}
		validateItemKeys(opts.hashKey, opts.sortKey);
		const hashKey = encodeHashKey(opts.hashKey);
		const sortKey = encodeSortKey(opts.sortKey);
		const { doId, partitionContext } = this.#options.topology.pickPartition(hashKey, sortKey);
		const stub = PartitionDO.get(env[this.#options.topology.partitionContext().ns], doId);
		// Encode data once at this boundary; the DO receives string | Uint8Array + kind.
		const { data, ...rest } = opts;
		const res = await stub.apiPutItem(partitionContext, { ...rest, hashKey, sortKey, ...encodeItemData(data) });
		// Echo the caller's original keys (no decode needed).
		return { item: { hashKey: opts.hashKey, sortKey: opts.sortKey }, version: res.version, meta: res.meta };
	}

	async getItem(opts: GetItemOptions): Promise<GetItemResult> {
		validateItemKeys(opts.hashKey, opts.sortKey);
		const hashKey = encodeHashKey(opts.hashKey);
		const sortKey = encodeSortKey(opts.sortKey);
		const { doId, partitionContext } = this.#options.topology.pickPartition(hashKey, sortKey);
		const stub = PartitionDO.get(env[this.#options.topology.partitionContext().ns], doId);
		const res = await stub.apiGetItem(partitionContext, { ...opts, hashKey, sortKey });
		// Echo the caller's original keys (no decode needed); preserve the found/not-found discriminant.
		// json data arrives as JSON text — parse it once here to the public JsonValue.
		if (res.found) {
			return {
				found: true,
				item: { ...res.item, hashKey: opts.hashKey, sortKey: opts.sortKey, data: decodeItemData(res.item.kind, res.item.data) },
				meta: res.meta,
			};
		}
		return { found: false, item: { hashKey: opts.hashKey, sortKey: opts.sortKey }, meta: res.meta };
	}

	async deleteItem(opts: DeleteItemOptions) {
		validateItemKeys(opts.hashKey, opts.sortKey);
		const hashKey = encodeHashKey(opts.hashKey);
		const sortKey = encodeSortKey(opts.sortKey);
		const { doId, partitionContext } = this.#options.topology.pickPartition(hashKey, sortKey);
		const stub = PartitionDO.get(env[this.#options.topology.partitionContext().ns], doId);
		const res = await stub.apiDeleteItem(partitionContext, { ...opts, hashKey, sortKey });
		// Echo the caller's original keys (no decode needed).
		return { item: { hashKey: opts.hashKey, sortKey: opts.sortKey }, deleted: res.deleted, meta: res.meta };
	}

	async transactWriteItems(opts: {
		operations: Array<{
			hashKey: string | Uint8Array;
			sortKey?: string | Uint8Array;
			operation: "put" | "delete" | "check";
			data?: string | Uint8Array | JsonComposite;
			conditions?: ItemCondition[];
		}>;
		clientRequestToken?: string;
	}): Promise<InitiateWriteResponse> {
		// Encode data once at this boundary (only puts carry data); the TC/DO see string | Uint8Array + kind.
		// Validation then runs on the already-encoded data so json payload accounting reuses this single
		// serialization rather than JSON.stringify-ing a second time.
		const encoded = opts.operations.map((op) => (op.data !== undefined ? encodeItemData(op.data) : undefined));
		validateTransactWriteOperations(opts.operations.map((op, i) => ({ ...op, data: encoded[i]?.data })));
		const operations: TCWriteOperation[] = opts.operations.map((op, i) => {
			const hashKey = encodeHashKey(op.hashKey);
			const sortKey = encodeSortKey(op.sortKey);
			const { partitionContext } = this.#options.topology.pickPartition(hashKey, sortKey);
			return { ...op, hashKey, sortKey, partitionContext, data: encoded[i]?.data, kind: encoded[i]?.kind };
		});

		// TODO: We need to catch DO errors and retry with a different idempotency token to route
		// to a different TC if the chosen one is overloaded or has failed. Tricky to do for writes though...
		const idempotencyToken = opts.clientRequestToken ?? crypto.randomUUID().replaceAll("-", "");
		return await this.#staticShardedTCs.one(idempotencyToken, async (tcStub: DurableObjectStub<TransactionCoordinatorDO>) => {
			return await tcStub.initiateWrite({ clientRequestToken: idempotencyToken, operations });
		});
	}

	async transactGetItems(opts: {
		items: Array<{ hashKey: string | Uint8Array; sortKey?: string | Uint8Array }>;
	}): Promise<InitiateReadResponse> {
		const items: TCReadItem[] = opts.items.map((item) => {
			validateItemKeys(item.hashKey, item.sortKey);
			const hashKey = encodeHashKey(item.hashKey);
			const sortKey = encodeSortKey(item.sortKey);
			const { partitionContext } = this.#options.topology.pickPartition(hashKey, sortKey);
			return { ...item, hashKey, sortKey, partitionContext };
		});

		const response = await tryWhile(
			async () => {
				// Read-only TCs are ephemeral — random UUID, no client idempotency token needed.
				// Even better using a different shard key each time to maximize chances of hitting different TCs if there is an overloaded one.
				return await this.#staticShardedTCs.one(crypto.randomUUID(), async (tcStub: DurableObjectStub<TransactionCoordinatorDO>) => {
					return await tcStub.initiateRead({ items });
				});
			},
			(err: unknown, nextAttempt: number) => isErrorRetryable(err) && nextAttempt <= 3,
		);

		// json data arrives as JSON text — parse it once here to the public JsonValue.
		if (response.outcome !== "committed") return response;
		return {
			...response,
			items: response.items.map((item) => (item.found ? { ...item, data: decodeItemData(item.kind, item.data) } : item)),
		};
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
			return {
				hashKey: encodeHashKey(q.hashKey),
				interval: normalizeSkInterval(q.sort, encodeSortKey),
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
			partitionMetas.push(...rpcResult.partitionMetas);
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
					if (!String(e).includes("__special_destroy_sentinel")) throw e;
				}
				console.warn(`Destroyed partition DO ${ctx.doName} (partitionId=${ctx.partitionId})`);
			},
		);

		return { ok: true };
	}
}
