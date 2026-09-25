import { env } from "cloudflare:workers";
import { tryWhile } from "durable-utils/retries";
import {
	DataKind,
	DecodedItemData,
	DeleteItemOptions,
	DeleteItemResult,
	EncodedItemData,
	GetItemOptions,
	GetItemResult,
	HashKey,
	JsonComposite,
	JsonValue,
	OperationMetrics,
	PartitionInfo,
	PutItemOptions,
	PutItemResult,
	ProjectedItem,
	QueryItemsMeta,
	QueryItemsOptions,
	QueryItemsProjectedOptions,
	QueryItemsProjectedResult,
	QueryItemsResult,
	QuerySelect,
	SortKey,
} from "../shared/types.js";
import { isDestroyAbortError } from "../shared/cf-utils.js";
import { partitionStubByName, txCoordinatorStubByName } from "../shared/do-stubs.js";
import { FOKOS_HASH_PARTITIONS_MAX } from "../sharding/route-context.js";
import { FokosRouter } from "../sharding/router.js";
import type {
	ExecutionFailureCode,
	RejectionReason,
	TransactGetItemKey,
	TransactGetItemsOptions,
	TransactGetItemsResult,
	TransactWriteItemsOptions,
	TransactWriteItemsResult,
	TransactWriteOperationResult,
} from "../shared/transaction-api-types.js";
import type {
	InitiateReadResponseEncoded,
	ReadForTransactionItemResultEncoded,
	RejectionReasonEncoded,
	SingleShotResponse,
	TCReadItem,
	TCWriteOperation,
	TransactWriteOperationResultEncoded,
} from "../shared/transaction-wire-types.js";
import {
	encodeHashKey,
	encodeSortBound,
	encodeSortKey,
	validateItemDataSize,
	validateItemKeys,
	validateReturnValuesOnConditionCheckFailure,
	singlePartitionTarget,
	validateTransactGetItemCount,
	validateTransactGetItemKeys,
	validateTransactWriteOperations,
	validateClientRequestToken,
	decodeItemKeys,
} from "../shared/transaction-limits.js";
import {
	CONFLICT_CODES,
	FokosConflictError,
	FokosError,
	FokosInternalError,
	FokosUnavailableError,
	FokosValidationError,
	INTERNAL_CODES,
	UNAVAILABLE_CODES,
	VALIDATION_CODES,
	isRuntimeRetryableError,
} from "../shared/errors.js";
import {
	CONDITION_CHECK_CODES,
	FokosConditionCheckError,
	FokosTransactionCancelledError,
	TRANSACTION_CANCELLED_CODES,
	withExpressionErrors,
} from "../shared/errors-operations.js";
import invariant from "../shared/invariant.js";
import { SHARDING_UNAVAILABLE_CODES } from "../sharding/errors.js";
import { KeyCodec } from "../sharding/key-codec.js";
import { attachRouting, routedError } from "../sharding/envelope.js";
import type { FokosPublicRouting } from "../sharding/runtime-types.js";
import { normalizeSkInterval } from "../sharding/sk-interval.js";
import { leafPartitionInfo, partitionInfoOf } from "./partition-info.js";
import type { ScanCursor, StoredItem } from "../shared/partition/partition-store.js";
import { CURSOR_VERSION, encodeCursor, decodeCursor, computeCursorFingerprint, type DecodedCursor } from "../shared/query/cursor.js";
import {
	DEFAULT_EVALUATED_ITEMS_PER_PAGE,
	DEFAULT_RESPONSE_BYTES_PER_PAGE,
	MAX_EVALUATED_BYTES_PER_PAGE,
	MAX_EVALUATED_ITEMS_PER_PAGE,
	MAX_PARTITION_VISITS_PER_PAGE,
	MAX_RESPONSE_BYTES_PER_PAGE,
	QueryPageBudget,
} from "../shared/query/page-budget.js";
import {
	compileConditionExpression,
	compileProjectionExpression,
	compileQueryExpression,
	compileUpdateExpression,
} from "../shared/expression/compiler.js";
import { projectedItemFromWireRow, type ProjectedWireRow } from "../shared/expression/projection.js";
import { RESERVED_SHARD_GROUP_PREFIX, type FokosDbPolicy, type FokosDbRouteContext } from "../shared/partition-context.js";

const TX_COORDINATORS_PER_ROOT_TREE = 2;

/**
 * How long `transactWriteItems` sends a write again while its coordinator answers `partition_migrating`.
 * It is longer than the 5-second fallback alarm of the runtime, so an import that a crash stopped can
 * finish inside it.
 */
const TX_COORDINATOR_MIGRATING_RETRY_MS = 15_000;

// The single JS↔wire encode boundary for item data: a Uint8Array is opaque bytes,
// a string is opaque text, and an object/array is JSON — stringified exactly once here
// so the DO only ever receives `string | Uint8Array` plus a kind discriminant.
function encodeItemData(data: string | Uint8Array | JsonComposite): EncodedItemData {
	if (data instanceof Uint8Array) {
		return { kind: "bytes", data };
	}
	if (typeof data === "string") {
		return { kind: "text", data };
	}
	// `JsonComposite` is arrays and objects only.
	// Accepting a primitive silently would make the declared type a lie, and taking it back later would be
	// breaking — whereas relaxing this check later is not.
	if (data === null || typeof data !== "object") {
		throw new FokosValidationError(VALIDATION_CODES.item_data_wrong_type, {
			message: "data must be an object, array, string or Uint8Array",
			attributes: { type: data === null ? "null" : typeof data },
		});
	}
	let text: string;
	try {
		text = JSON.stringify(data);
	} catch (err) {
		// A circular reference or a BigInt. Only JSON.stringify knows which, so the cause keeps its error.
		throw new FokosValidationError(VALIDATION_CODES.item_data_not_json_serializable, {
			message: "data is not JSON-serializable",
			cause: err,
		});
	}
	// The guard above rules out every value that JSON.stringify drops, with one exception: a `toJSON`
	// that itself returns undefined (or a function, or a symbol) makes the WHOLE document undefined.
	if (text === undefined) {
		throw new FokosValidationError(VALIDATION_CODES.item_data_not_json_serializable, {
			message: "data is not JSON-serializable (its toJSON() returned undefined)",
		});
	}
	return { kind: "json", data: text };
}

// The matching decode boundary: json rows arrive from the DO as JSON text, parsed once back to a
// JsonValue; bytes/text pass through untouched. A parse failure means the stored JSONB → json() text
// is malformed (a store/encoding bug, not user input), so surface it loudly rather than returning junk.
function decodeItemData(kind: DataKind, data: string | Uint8Array | JsonValue): DecodedItemData {
	// The store writes `data_kind` beside the value, so the pair is always the one that was written.
	if (kind !== "json") {
		return { kind, data } as DecodedItemData;
	}
	try {
		return { kind, data: JSON.parse(data as string) as JsonValue };
	} catch (err) {
		console.error({
			message: "fokos: failed to parse json item data returned by the store",
			error: String(err),
			errorProps: err,
		});
		throw new FokosInternalError(INTERNAL_CODES.item_data_parse_failed, {
			message: "failed to parse json item data returned by the store",
			cause: err,
		});
	}
}

function decodeRejectionReason(reason: RejectionReasonEncoded): RejectionReason {
	if (reason.code === "condition_failed" && reason.item) {
		return {
			...reason,
			item: {
				...reason.item,
				...decodeItemData(reason.item.kind, reason.item.data),
			},
		};
	}
	return reason as RejectionReason;
}

/** The error `putItem` and `deleteItem` raise when the partition rejects the condition. */
function conditionCheckError(
	keys: { hashKey: HashKey; sortKey?: SortKey },
	res: { reason: RejectionReasonEncoded; meta: OperationMetrics },
	routing: FokosPublicRouting,
): FokosConditionCheckError {
	const reason = decodeRejectionReason(res.reason);
	invariant(reason.code === "condition_failed", "an item RPC rejects only a failed condition");
	return new FokosConditionCheckError(CONDITION_CHECK_CODES.condition_failed, {
		message: "condition failed",
		attributes: { hashKey: keys.hashKey, sortKey: keys.sortKey },
		reason,
		meta: publicMeta(res.meta, routing),
	});
}

/** The error `transactWriteItems` raises for a cancelled transaction, on either path. */
function transactionCancelledError(fields: {
	transactionId: string;
	idempotencyToken: string;
	results: TransactWriteOperationResultEncoded[];
}): FokosTransactionCancelledError {
	return new FokosTransactionCancelledError(TRANSACTION_CANCELLED_CODES.transaction_cancelled, {
		message: "transaction cancelled",
		attributes: { transactionId: fields.transactionId, idempotencyToken: fields.idempotencyToken },
		results: fields.results.map(decodeOperationResult),
	});
}

function decodeOperationResult(res: TransactWriteOperationResultEncoded): TransactWriteOperationResult {
	if (res.outcome === "passed") {
		return { outcome: "passed" };
	}
	if (res.outcome === "not_evaluated") {
		return { outcome: "not_evaluated" };
	}
	return {
		outcome: "rejected",
		reason: decodeRejectionReason(res.reason),
		...(res.itemOmitted ? { itemOmitted: res.itemOmitted } : {}),
	};
}

/** Runs the body of a public method, and raises any error from it as a FokosError. */
/** `transactGetItems` raises it when a requested item holds a pending write of an in-progress transaction. */
function pendingWriteError(): FokosConflictError {
	return new FokosConflictError(CONFLICT_CODES.pending_write, { message: "an item has a pending write of an in-progress transaction" });
}

async function withFokosErrors<T>(fn: () => Promise<T>): Promise<T> {
	try {
		return await fn();
	} catch (e) {
		const err = mapInternalErrorToPublic(FokosError.wrap(e));
		// A partition attaches its routing to its error. The routing state stops here, as it does on a
		// result: the public error carries the same `meta` a result would, and the internal hints go.
		const routed = routedError(err);
		if (routed) {
			const meta = partitionInfoOf(routed.routing);
			delete (routed as { routing?: unknown }).routing;
			Object.assign(routed, { meta });
		}
		throw err;
	}
}

/**
 * Translates an internal condition that reached the public boundary into the code a client already
 * knows how to handle.
 *
 * With `repartition_not_cut_over`, the repartition protocol tells a target that its source still
 * owns the slice. A client has no repartitions in its vocabulary, and the condition means to it what
 * `partition_migrating` means: retry shortly. The internal code stays in `attributes.runtimeCode`,
 * and the error keeps its original `error_id`, so one log line still joins the two ends.
 */
function mapInternalErrorToPublic(err: FokosError): FokosError {
	if (err.code !== SHARDING_UNAVAILABLE_CODES.repartition_not_cut_over.code) {
		return err;
	}
	const mapped = new FokosUnavailableError(UNAVAILABLE_CODES.partition_migrating, {
		message: "partition split in progress, please retry later",
		error_id: err.error_id,
		cause: err.cause,
		attributes: { ...err.attributes, runtimeCode: err.code },
	});
	// The routing is an own property of the error object, so a new object loses it. It must move with
	// the mapping. This code would otherwise be the only one that reaches a client with no meta.
	const routed = routedError(err);
	return routed ? attachRouting(mapped, routed.routing) : mapped;
}

function validateTtlAt(ttlAt: number | undefined, where: string): void {
	if (ttlAt === undefined) {
		return;
	}
	if (!Number.isInteger(ttlAt) || ttlAt <= 0) {
		throw new FokosValidationError(VALIDATION_CODES.ttl_at_invalid, {
			message: "ttlAt must be an integer greater than zero",
			attributes: { api: where, ttlAt },
		});
	}
}

export type FokosDBOptions = {
	/** The router of the table: its topology, range config and policy. */
	topology: FokosRouter<FokosDbPolicy>;

	/**
	 * The root coordinators of the table. Defaults to two per root partition, with a maximum of
	 * `FOKOS_HASH_PARTITIONS_MAX`. Each root splits by hash when it grows larger than `hashSplitConditions.maxSizeMb` of
	 * the table, or larger than the size limit of a coordinator. Thus the pool grows automatically.
	 * The value must not change for a table that exists, as `rootTreesN` must not.
	 */
	coordinatorRootsN?: number;

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

/** The public meta of one result: the metrics of the work, and the partition that produced it. */
function publicMeta(metrics: OperationMetrics, routing: FokosPublicRouting): OperationMetrics & PartitionInfo {
	return { ...metrics, ...partitionInfoOf(routing) };
}

export class FokosDB {
	#options: Required<FokosDBOptions>;
	/** The router of the coordinator group of the table, `fokos.tc.<shardGroup>`. */
	#coordinators: FokosRouter<FokosDbPolicy>;

	constructor(options: FokosDBOptions) {
		const { topology, rangeConfig, policy } = options.topology;
		// The default has the same maximum as the check below, so a large `rootTreesN` does not fail.
		this.#options = {
			...options,
			coordinatorRootsN:
				options.coordinatorRootsN ?? Math.min(TX_COORDINATORS_PER_ROOT_TREE * topology.rootTreesN, FOKOS_HASH_PARTITIONS_MAX),
			singlePartitionFastPath: options.singlePartitionFastPath ?? true,
		};
		const { coordinatorRootsN } = this.#options;
		if (!Number.isInteger(coordinatorRootsN) || coordinatorRootsN < 1 || coordinatorRootsN > FOKOS_HASH_PARTITIONS_MAX) {
			throw new FokosValidationError(VALIDATION_CODES.num_tx_coordinators_invalid, {
				message: `coordinatorRootsN must be an integer between 1 and ${FOKOS_HASH_PARTITIONS_MAX}`,
				attributes: { coordinatorRootsN },
			});
		}
		// The coordinators take the topology of the table except its shard group and its number of roots:
		// the same hash split fan-out, and the same jurisdiction. The range config is not used, because
		// a coordinator has no range tree.
		this.#coordinators = new FokosRouter(
			{ ...topology, shardGroup: `${RESERVED_SHARD_GROUP_PREFIX}tc.${topology.shardGroup}`, rootTreesN: coordinatorRootsN },
			rangeConfig,
			policy,
		);
	}

	options() {
		return { ...this.#options };
	}

	// Each public method wraps its body, so every error that leaves FokosDB is a FokosError.

	async putItem(opts: PutItemOptions): Promise<PutItemResult> {
		return await withFokosErrors(async () => await this.#putItem(opts));
	}

	// `T` types the json value and the projected record of the result. The store holds opaque data, so
	// the library cannot check `T`: it is the caller's own statement about what the item holds.
	async getItem<T = never>(opts: GetItemOptions): Promise<GetItemResult<T>> {
		return (await withFokosErrors(async () => await this.#getItem(opts))) as GetItemResult<T>;
	}

	async deleteItem(opts: DeleteItemOptions): Promise<DeleteItemResult> {
		return await withFokosErrors(async () => await this.#deleteItem(opts));
	}

	async transactWriteItems(opts: TransactWriteItemsOptions): Promise<TransactWriteItemsResult> {
		return await withFokosErrors(async () => await this.#transactWriteItems(opts));
	}

	// `Ts` types each item by position, so one request can read unrelated items and answer each with its
	// own type. The store holds opaque data, so the library cannot check `Ts`: it is the caller's own
	// statement about what each item holds.
	async transactGetItems<Ts extends readonly unknown[] = never[]>(
		opts: NoInfer<TransactGetItemsOptions<Ts>>,
	): Promise<TransactGetItemsResult<Ts>> {
		return (await withFokosErrors(async () => await this.#transactGetItems(opts))) as TransactGetItemsResult<Ts>;
	}

	async queryItems<T = never>(opts: QueryItemsProjectedOptions): Promise<QueryItemsProjectedResult<T>>;
	async queryItems<T = never>(opts: QueryItemsOptions): Promise<QueryItemsResult<T>>;
	async queryItems<T = never>(opts: QueryItemsOptions): Promise<QueryItemsResult<T> | QueryItemsProjectedResult<T>> {
		return (await withFokosErrors(async () => await this.#queryItems(opts))) as QueryItemsResult<T> | QueryItemsProjectedResult<T>;
	}

	/** Stops at the first failure. A partial destroy stays partial, and a later call continues it. */
	async destroy(): Promise<{ ok: true }> {
		return await withFokosErrors(async () => await this.#destroy());
	}

	async #putItem(opts: PutItemOptions): Promise<PutItemResult> {
		validateTtlAt(opts.ttlAt, "putItem");
		validateItemKeys(opts.hashKey, opts.sortKey);
		validateReturnValuesOnConditionCheckFailure(opts.returnValuesOnConditionCheckFailure);
		const hashKey = encodeHashKey(opts.hashKey);
		const sortKey = encodeSortKey(opts.sortKey);
		// Encode data once at this boundary; the DO receives string | Uint8Array + kind.
		const encoded = encodeItemData(opts.data);
		const condition = opts.condition ? withExpressionErrors(() => compileConditionExpression(opts.condition!)) : undefined;
		// Measured on the ENCODED form, so a json payload is capped by the text actually stored and
		// the same item is accepted or rejected identically here and in transactWriteItems.
		validateItemDataSize(encoded.data, "putItem");
		const partitionContext = this.#options.topology.rootContext(hashKey);
		const stub = partitionStubByName(env, partitionContext, partitionContext.doName);
		const { value: res, routing } = this.#options.topology.unwrap(
			await stub.apiPutItem(partitionContext, {
				hashKey,
				sortKey,
				data: encoded.data,
				kind: encoded.kind,
				ttlAt: opts.ttlAt,
				condition,
				returnValuesOnConditionCheckFailure: opts.returnValuesOnConditionCheckFailure,
			}),
		);
		if (res.outcome === "rejected") {
			throw conditionCheckError(opts, res, routing);
		}
		// The DO returns no keys; the caller's own are the only ones it can recognise.
		return { item: { hashKey: opts.hashKey, sortKey: opts.sortKey }, version: res.version, meta: publicMeta(res.meta, routing) };
	}

	async #getItem(opts: GetItemOptions): Promise<GetItemResult> {
		validateItemKeys(opts.hashKey, opts.sortKey);
		const hashKey = encodeHashKey(opts.hashKey);
		const sortKey = encodeSortKey(opts.sortKey);
		const projection =
			opts.projection === undefined ? undefined : withExpressionErrors(() => compileProjectionExpression(opts.projection!));
		const partitionContext = this.#options.topology.rootContext(hashKey);
		const stub = partitionStubByName(env, partitionContext, partitionContext.doName);
		const { value: res, routing } = this.#options.topology.unwrap(
			await stub.apiGetItem(partitionContext, { hashKey, sortKey, ...(projection === undefined ? {} : { projection }) }),
		);
		const meta = publicMeta(res.meta, routing);
		// The DO returns no keys; supply the caller's own and preserve the found/not-found discriminant.
		// json data arrives as JSON text — parse it once here to the public JsonValue.
		if (res.found) {
			if (res.item.kind === "projected") {
				invariant(projection, "fokos/getItem: projected response without a projection plan");
				return {
					found: true,
					item: {
						hashKey: opts.hashKey,
						sortKey: opts.sortKey,
						data: projectedItemFromWireRow(projection.names, res.item.projected),
						kind: "projected",
						...(res.item.ttlAt === undefined ? {} : { ttlAt: res.item.ttlAt }),
						version: res.item.version,
					},
					meta,
				};
			}
			return {
				found: true,
				item: { hashKey: opts.hashKey, sortKey: opts.sortKey, ...res.item, ...decodeItemData(res.item.kind, res.item.data) },
				meta,
			};
		}
		return { found: false, item: { hashKey: opts.hashKey, sortKey: opts.sortKey }, meta };
	}

	async #deleteItem(opts: DeleteItemOptions): Promise<DeleteItemResult> {
		validateItemKeys(opts.hashKey, opts.sortKey);
		validateReturnValuesOnConditionCheckFailure(opts.returnValuesOnConditionCheckFailure);
		const hashKey = encodeHashKey(opts.hashKey);
		const sortKey = encodeSortKey(opts.sortKey);
		const condition = opts.condition ? withExpressionErrors(() => compileConditionExpression(opts.condition!)) : undefined;
		const partitionContext = this.#options.topology.rootContext(hashKey);
		const stub = partitionStubByName(env, partitionContext, partitionContext.doName);
		const { value: res, routing } = this.#options.topology.unwrap(
			await stub.apiDeleteItem(partitionContext, {
				hashKey,
				sortKey,
				condition,
				returnValuesOnConditionCheckFailure: opts.returnValuesOnConditionCheckFailure,
			}),
		);
		if (res.outcome === "rejected") {
			throw conditionCheckError(opts, res, routing);
		}
		// The DO returns no keys; the caller's own are the only ones it can recognise.
		return { item: { hashKey: opts.hashKey, sortKey: opts.sortKey }, deleted: res.deleted, meta: publicMeta(res.meta, routing) };
	}

	async #transactWriteItems(opts: TransactWriteItemsOptions): Promise<TransactWriteItemsResult> {
		if (opts.clientRequestToken !== undefined) {
			validateClientRequestToken(opts.clientRequestToken);
		}

		// Encode each put, compile each update, and compile each condition once at this boundary. A `data`
		// field set on a non-put by a non-TypeScript caller stays present so validation rejects it.
		const prepared = opts.items.map((item) => {
			const condition = item.condition ? withExpressionErrors(() => compileConditionExpression(item.condition!)) : undefined;
			if (item.operation === "update") {
				validateTtlAt(item.ttlAt, "transactWriteItems");
				const update = withExpressionErrors(() => compileUpdateExpression(item.update));
				return { ...item, update, condition };
			}
			if (item.operation !== "put") {
				return { ...item, condition };
			}
			validateTtlAt(item.ttlAt, "transactWriteItems");
			return { ...item, ...encodeItemData(item.data), condition };
		});
		// Validation encodes each key exactly once and hands the canonical bytes back in input order.
		const keys = validateTransactWriteOperations(prepared);
		const items: TCWriteOperation[] = prepared.map((item, i) => {
			const { hashKey, sortKey } = keys[i];
			const partitionContext = this.#options.topology.rootContext(hashKey);
			return { ...item, opIndex: i, hashKey, sortKey, partitionContext };
		});

		if (!opts.clientRequestToken) {
			// A transaction that carries a client request token does not use this path: an idempotent replay
			// is answered from the coordinator's ledger, and a partition keeps no record of finished
			// transactions.
			//
			// TODO: give the partition its own completed-transaction-token storage. Once a partition can
			// recognise a token it has already executed and return that outcome, this restriction lifts and
			// token-bearing single-partition transactions can take the same single round trip.
			const fastPathResult = await this.#writeSingleShotFastPath(items);
			if (fastPathResult) {
				return fastPathResult;
			}
		}

		// The token is the route key of the coordinator, so a request always carries one.
		const idempotencyToken = opts.clientRequestToken ?? crypto.randomUUID().replaceAll("-", "");
		const ctx = this.#coordinators.rootContext(encodeHashKey(idempotencyToken));

		// A coordinator answers `partition_migrating` while it splits: its root forwards the request to
		// the child that owns the token, and that child refuses it until its import is complete. The
		// request carries the token, so a retry resumes the same transaction and never starts a second one.
		const deadline = Date.now() + TX_COORDINATOR_MIGRATING_RETRY_MS;
		// The TC response carries no keys — nothing to decode at this boundary, unlike every other
		// method here. See TransactWriteItemsResult.
		const encoded = await tryWhile(
			async () =>
				(await txCoordinatorStubByName(env, ctx, ctx.doName).initiateWrite(ctx, { clientRequestToken: idempotencyToken, items })).value,
			(err) => FokosError.isCode(err, UNAVAILABLE_CODES.partition_migrating) && Date.now() < deadline,
			{ baseDelayMs: 100, maxDelayMs: 2_000 },
		);
		// The outcome is the driver's, not the caller's: a committed transaction is the only value this
		// method returns, and a cancelled one raises instead.
		if (encoded.outcome === "committed") {
			return { transactionId: encoded.transactionId, idempotencyToken: encoded.idempotencyToken };
		}
		throw transactionCancelledError(encoded);
	}

	/**
	 * One round trip to the owning partition when it owns every item, which applies the whole set
	 * atomically. Returns null when the fast path does not apply, so the caller runs the coordinator
	 * path: the option is off, the transaction carries a token, the client hint says the items span
	 * partitions, or the partition itself answered `not_applicable` because they do.
	 *
	 * `transactionId` is generated here, as the coordinator would generate it: nothing on this path
	 * stores it, and it exists only so the public response shape is the same on both paths.
	 */
	async #writeSingleShotFastPath(items: TCWriteOperation[]): Promise<TransactWriteItemsResult | null> {
		if (!this.#options.singlePartitionFastPath) {
			return null;
		}

		const target = singlePartitionTarget(items);
		if (!target) {
			return null;
		}

		const transactionId = crypto.randomUUID().replaceAll("-", "");
		const stub = partitionStubByName(env, target, target.doName);
		const request = { items: items.map(({ partitionContext: _partitionContext, ...item }) => item) };

		let response: SingleShotResponse;
		try {
			// No retry, matching the coordinator path, which does not retry a write either.
			response = (await stub.txExecuteSingleShot(target, request)).value;
		} catch (err) {
			// The partition does not throw after its apply commits, so an error that partition code raised
			// means nothing applied: the transaction cancelled, and that one partition owns every operation,
			// as the coordinator reports the same refusal of a prepare. A foreign error can be a reply lost
			// after the apply, so its outcome is unknown.
			if (FokosError.is(err) && !FokosError.isCode(err, INTERNAL_CODES.foreign_error)) {
				throw transactionCancelledError({
					transactionId,
					idempotencyToken: transactionId,
					results: items.map((item) => ({
						outcome: "rejected",
						reason: { code: err.code as ExecutionFailureCode, ...decodeItemKeys(item.hashKey, item.sortKey), error_id: err.error_id },
					})),
				});
			}
			throw err;
		}

		// No single partition owns every item. Nothing was written, so the coordinator path runs instead.
		if (response.outcome === "not_applicable") {
			return null;
		}
		if (response.outcome === "committed") {
			return { transactionId, idempotencyToken: transactionId };
		}
		throw transactionCancelledError({ transactionId, idempotencyToken: transactionId, ...response });
	}

	// The positional types of the public method are erased here: every item takes the same path, and only
	// the caller knows which type belongs to which position.
	async #transactGetItems(opts: { items: readonly TransactGetItemKey[] }): Promise<TransactGetItemsResult> {
		validateTransactGetItemCount(opts.items.length);
		// Each item is built explicitly: the raw projection AST never crosses the RPC boundary, only the
		// compiled plan does, and only when the caller asked for one.
		const items: TCReadItem[] = opts.items.map((item) => {
			validateItemKeys(item.hashKey, item.sortKey);
			const hashKey = encodeHashKey(item.hashKey);
			const sortKey = encodeSortKey(item.sortKey);
			const projection =
				item.projection === undefined ? undefined : withExpressionErrors(() => compileProjectionExpression(item.projection!));
			const partitionContext = this.#options.topology.rootContext(hashKey);
			return { hashKey, sortKey, partitionContext, ...(projection === undefined ? {} : { projection }) };
		});
		validateTransactGetItemKeys(items);

		// TODO: Make the two-phase driver location configurable. A global caller Worker can be far from the
		// data partitions, so a coordinator near those partitions can reduce repeated cross-region trips.
		const response = (await this.#readSnapshotFastPath(items)) ?? (await this.#readTransaction(items));

		// The public boundary — the single exit where the internal representation becomes the public one:
		// decode the KeyBytes back to public keys (the empty sentinel maps to an absent sortKey, same as
		// queryItems), parse json text once into a JsonValue, and drop the read-transaction bookkeeping
		// (deleteRevision / hasPendingWrite) so callers never depend on it. Those two are meaningless in a
		// committed read regardless — the driver raises an error when any item has a pending write.
		return {
			items: response.items.map((encoded, index) => {
				const { deleteRevision: _deleteRevision, hasPendingWrite: _hasPendingWrite, hashKey, sortKey, ...item } = encoded;
				const keys = {
					hashKey: KeyCodec.decode(hashKey),
					sortKey: sortKey.byteLength === 0 ? undefined : KeyCodec.decode(sortKey),
				};
				if (!item.found) {
					return { ...keys, ...item };
				}
				if (item.kind === "projected") {
					// items[i] answers request.items[i], so the record's names come from that item's own plan.
					const plan = items[index].projection;
					invariant(plan, "fokos/transactGetItems: projected result without a projection plan");
					return {
						...keys,
						found: true as const,
						data: projectedItemFromWireRow(plan.names, item.projected),
						kind: "projected" as const,
						version: item.version,
						...(item.ttlAt === undefined ? {} : { ttlAt: item.ttlAt }),
					};
				}
				return { ...keys, ...item, ...decodeItemData(item.kind, item.data) };
			}),
		};
	}

	/**
	 * One round trip to the owning partition when every requested key resolves to it. Returns null
	 * when the fast path does not apply, so the caller runs the two-phase path: either the client hint
	 * says the keys span partitions, or the partition itself answered `not_applicable` because they do.
	 */
	async #readSnapshotFastPath(items: TCReadItem[]): Promise<InitiateReadResponseEncoded | null> {
		if (!this.#options.singlePartitionFastPath) {
			return null;
		}
		const target = singlePartitionTarget(items);
		if (!target) {
			return null;
		}

		const stub = partitionStubByName(env, target, target.doName);
		const request = {
			items: items.map(({ hashKey, sortKey, projection }) => ({ hashKey, sortKey, ...(projection === undefined ? {} : { projection }) })),
		};
		// Every error, a transport failure included, is the caller's, exactly as on the two-phase path.
		const response = await tryWhile(
			async () => (await stub.txReadSnapshot(target, request)).value,
			(err: unknown, nextAttempt: number) => isRuntimeRetryableError(err) && nextAttempt <= 3,
		);
		// No single partition owns every key. Nothing was read, so the two-phase path runs instead.
		if (response.outcome === "not_applicable") {
			return null;
		}
		if (response.outcome === "aborted") {
			throw pendingWriteError();
		}
		return response;
	}

	async #readTransaction(requestedItems: TCReadItem[]): Promise<InitiateReadResponseEncoded> {
		const transactionId = crypto.randomUUID().replaceAll("-", "");

		// Group items by partition, keeping the context alongside.
		const partitionMap = new Map<string, { pCtx: FokosDbRouteContext; items: TCReadItem[] }>();
		for (const item of requestedItems) {
			const doName = item.partitionContext.doName;
			let entry = partitionMap.get(doName);
			if (!entry) {
				entry = { pCtx: item.partitionContext, items: [] };
				partitionMap.set(doName, entry);
			}
			entry.items.push(item);
		}
		const partitionEntries = [...partitionMap.values()];

		// Phase 1
		const phase1Settled = await Promise.allSettled(
			partitionEntries.map(({ pCtx, items }) =>
				tryWhile(
					async () =>
						await partitionStubByName(env, pCtx, pCtx.doName).txReadForTransaction(pCtx, {
							transactionId,
							items: items.map((i) => ({
								hashKey: i.hashKey,
								sortKey: i.sortKey,
								...(i.projection === undefined ? {} : { projection: i.projection }),
							})),
						}),
					(_err, nextAttempt) => nextAttempt <= 5,
					{ baseDelayMs: 100, maxDelayMs: 2_000 },
				),
			),
		);

		const phase1Flat: ReadForTransactionItemResultEncoded[] = [];
		for (const r of phase1Settled) {
			// A read applies nothing, so the error of a failed phase call is the answer, as the partition raised it.
			if (r.status === "rejected") {
				throw r.reason;
			}
			phase1Flat.push(...r.value.value.items);
		}

		if (phase1Flat.some((item) => item.hasPendingWrite)) {
			throw pendingWriteError();
		}

		// Phase 2 — verify no concurrent mutations
		const phase2Settled = await Promise.allSettled(
			partitionEntries.map(({ pCtx, items }) =>
				tryWhile(
					async () =>
						await partitionStubByName(env, pCtx, pCtx.doName).txReadForTransaction(pCtx, {
							transactionId,
							items: items.map((i) => ({
								hashKey: i.hashKey,
								sortKey: i.sortKey,
								...(i.projection === undefined ? {} : { projection: i.projection }),
							})),
						}),
					(_err, nextAttempt) => nextAttempt <= 5,
					{ baseDelayMs: 100, maxDelayMs: 2_000 },
				),
			),
		);

		const phase2Flat: ReadForTransactionItemResultEncoded[] = [];
		for (const r of phase2Settled) {
			if (r.status === "rejected") {
				throw r.reason;
			}
			phase2Flat.push(...r.value.value.items);
		}

		if (phase2Flat.some((item) => item.hasPendingWrite)) {
			throw pendingWriteError();
		}

		// Pair the two phases by key, not by position: PartitionDO fans items out to child partitions and
		// flattens the replies, so result order is not request order. KeyCodec.pairKey is the ONE identity
		// primitive for a (hashKey, sortKey) pair — the same one commitLocal's keyset check uses. It
		// returns a bigint, a primitive, so Map lookup compares by value.
		const itemIdentity = (r: ReadForTransactionItemResultEncoded): bigint => KeyCodec.pairKey(r.hashKey, r.sortKey);

		// Did both phases observe the same committed state? `version` (the item's `v`) is the primary
		// datum: a monotonic per-item counter, so unlike a wall-clock timestamp it cannot miss two writes
		// landing inside the same millisecond. This mirrors the LSN comparison the DynamoDB paper uses
		// for its read transactions. `deleteRevision` is the owner partition's user-delete counter: it
		// catches a delete-and-recreate that lands back on the same version, and an absent-create-delete
		// sequence. An unrelated user delete in the same partition is a conservative conflict. An item
		// absent in both phases compares equal and is not a conflict. Item timestamps are not compared.
		const sameCommittedState = (a: ReadForTransactionItemResultEncoded, b: ReadForTransactionItemResultEncoded): boolean => {
			if (a.found !== b.found) {
				return false;
			}
			if (a.found && b.found && a.version !== b.version) {
				return false;
			}
			return a.deleteRevision === b.deleteRevision;
		};

		// Walk the REQUEST, not the replies: the response is positionally matched to request.items, so
		// the caller reads result[i] as the answer to items[i] instead of re-matching on keys. Neither
		// the partition grouping above nor the fan-out inside a PartitionDO preserves order, so the
		// request order is restored here, once, from the same pairKey identity.
		const phase1ByKey = new Map<bigint, ReadForTransactionItemResultEncoded>();
		for (const r of phase1Flat) {
			phase1ByKey.set(itemIdentity(r), r);
		}
		const phase2ByKey = new Map<bigint, ReadForTransactionItemResultEncoded>();
		for (const r of phase2Flat) {
			phase2ByKey.set(itemIdentity(r), r);
		}
		const items: ReadForTransactionItemResultEncoded[] = [];
		for (const requested of requestedItems) {
			const key = KeyCodec.pairKey(requested.hashKey, requested.sortKey);
			const p1 = phase1ByKey.get(key);
			const p2 = phase2ByKey.get(key);
			// A requested key with no reply means a participant dropped it — never expected, and not
			// something to answer with a short array.
			invariant(p1 && p2, "a participant of a read transaction dropped a requested key");
			if (!sameCommittedState(p1, p2)) {
				throw new FokosConflictError(CONFLICT_CODES.read_conflict, {
					message: "a write changed an item between the two phases of the read",
					attributes: decodeItemKeys(requested.hashKey, requested.sortKey),
				});
			}
			items.push(p1);
		}

		return { outcome: "committed", items };
	}

	async #queryItems(opts: QueryItemsOptions): Promise<QueryItemsResult | QueryItemsProjectedResult> {
		if (opts.queries.length === 0) {
			throw new FokosValidationError(VALIDATION_CODES.query_queries_empty, { message: "queries must not be empty" });
		}
		if (opts.limit !== undefined && (!Number.isSafeInteger(opts.limit) || opts.limit <= 0)) {
			throw new FokosValidationError(VALIDATION_CODES.query_limit_invalid, {
				message: "limit must be a positive integer when provided",
				attributes: { limit: opts.limit },
			});
		}
		if (opts.maxResponseBytes !== undefined && (!Number.isSafeInteger(opts.maxResponseBytes) || opts.maxResponseBytes <= 0)) {
			throw new FokosValidationError(VALIDATION_CODES.query_max_response_bytes_invalid, {
				message: "maxResponseBytes must be a positive integer when provided",
				attributes: { maxResponseBytes: opts.maxResponseBytes },
			});
		}
		const select: QuerySelect = opts.select ?? "projection";
		if (select !== "projection" && select !== "count") {
			throw new FokosValidationError(VALIDATION_CODES.query_select_invalid, {
				message: 'select must be "projection" or "count" when provided',
				attributes: { select: opts.select },
			});
		}
		// Count mode returns no item, so a projection has no meaning there.
		if (select === "count" && opts.projection !== undefined) {
			throw new FokosValidationError(VALIDATION_CODES.query_projection_with_count, {
				message: 'a projection is not valid with select "count"',
			});
		}
		const plan =
			opts.filter === undefined && opts.projection === undefined
				? null
				: withExpressionErrors(() => compileQueryExpression({ filter: opts.filter, projection: opts.projection }));

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
		const fingerprint = computeCursorFingerprint(normalizedQueries, plan?.filterIdentity ?? null, plan?.projectionIdentity ?? null);

		const budget = new QueryPageBudget({
			remainingEvaluatedItems: Math.min(opts.limit ?? DEFAULT_EVALUATED_ITEMS_PER_PAGE, MAX_EVALUATED_ITEMS_PER_PAGE),
			remainingEvaluatedBytes: MAX_EVALUATED_BYTES_PER_PAGE,
			remainingResponseBytes: Math.min(opts.maxResponseBytes ?? DEFAULT_RESPONSE_BYTES_PER_PAGE, MAX_RESPONSE_BYTES_PER_PAGE),
			remainingPartitionVisits: MAX_PARTITION_VISITS_PER_PAGE,
			allowOversizedFirstItem: true,
		});

		let startQueryIdx = 0;
		let startInner: DecodedCursor["inner"] = null;
		if (opts.cursor !== undefined) {
			const decoded = decodeCursor(opts.cursor);
			if (decoded.queryIdx >= normalizedQueries.length) {
				throw new FokosValidationError(VALIDATION_CODES.cursor_query_index_out_of_range, {
					message: "cursor queryIdx out of range",
					attributes: { queryIdx: decoded.queryIdx, queries: normalizedQueries.length },
				});
			}
			if (decoded.direction !== normalizedQueries[decoded.queryIdx].cursorDirection) {
				throw new FokosValidationError(VALIDATION_CODES.cursor_direction_mismatch, {
					message: "cursor direction mismatch — scanIndexForward differs from the page that issued this cursor",
				});
			}
			if (decoded.fingerprint !== fingerprint) {
				throw new FokosValidationError(VALIDATION_CODES.cursor_fingerprint_mismatch, {
					message: "cursor fingerprint mismatch — re-send the same request",
				});
			}
			startQueryIdx = decoded.queryIdx;
			startInner = decoded.inner;
		}

		const items: Array<QueryItemsResult["items"][number] | ProjectedItem> = [];
		const partitionMetas: QueryItemsResult["partitionMetas"] = [];
		let count = 0;
		let scannedCount = 0;
		let rowsReturned = 0;
		let forwardCount = 0;
		// The aggregates count every leaf that answered, and never only the leaves that `partitionMetas`
		// could name: the route list is capped, so naming a leaf is best effort while its counters are not.
		let rowsRead = 0;
		let partitionsVisited = 0;
		let cursor: string | undefined;

		for (let qi = startQueryIdx; qi < normalizedQueries.length; qi++) {
			const query = normalizedQueries[qi];
			if (query.interval === null) {
				continue;
			}

			const rpcCursor: ScanCursor | null =
				qi === startQueryIdx && startInner !== null
					? { hk: startInner.hashKey, sk: startInner.sortKey, inclusive: startInner.inclusive }
					: null;

			const partitionContext = this.#options.topology.rootContext(query.hashKey);
			const stub = partitionStubByName(env, partitionContext, partitionContext.doName);

			const { value: rpcResult, routing } = this.#options.topology.unwrap(
				await stub.apiQueryItems(partitionContext, {
					hashKey: query.hashKey,
					interval: query.interval,
					direction: query.direction,
					remainingEvaluatedItems: budget.remainingEvaluatedItems,
					remainingEvaluatedBytes: budget.remainingEvaluatedBytes,
					remainingResponseBytes: budget.remainingResponseBytes,
					remainingPartitionVisits: budget.remainingPartitionVisits,
					allowOversizedFirstItem: budget.allowOversizedFirstItem,
					cursor: rpcCursor,
					select,
					plan,
				}),
			);

			count += rpcResult.count;
			scannedCount += rpcResult.scannedCount;
			rowsReturned += rpcResult.rowsReturned;
			if (select === "projection") {
				if (plan?.projection) {
					for (const item of rpcResult.items) {
						items.push(projectedItemFromWireRow(plan.projection.names, item as ProjectedWireRow));
					}
				} else {
					for (const item of rpcResult.items) {
						const stored = item as StoredItem;
						items.push({
							hashKey: KeyCodec.decode(stored.hk),
							sortKey: stored.sk.byteLength === 0 ? undefined : KeyCodec.decode(stored.sk),
							// json data arrives as JSON text and is parsed once here to the public JsonValue.
							...decodeItemData(stored.kind, stored.data),
							ttlAt: stored.ttl_epoch_utc_seconds ?? undefined,
							version: stored.v,
						});
					}
				}
			}
			for (const leaf of rpcResult.partitionMetas) {
				rowsRead += leaf.rowsRead;
				partitionsVisited += 1;
				const info = leafPartitionInfo(leaf, routing);
				if (info) {
					partitionMetas.push(info);
				}
			}
			forwardCount += routing.forwardCount;
			budget.consume(rpcResult);

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
					console.warn("fokos/queryItems: remainingPartitionVisits budget exhausted across sub-queries, paginating early");
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

		const meta: QueryItemsMeta = { rowsRead, rowsReturned, forwardCount, partitionsVisited };

		return { items, count, scannedCount, cursor, meta, partitionMetas } as QueryItemsResult | QueryItemsProjectedResult;
	}

	async #destroy(): Promise<{ ok: true }> {
		// Coordinators first, partitions second. A transaction still in flight is driven BY a coordinator,
		// so wiping the coordinators stops the drivers before the data goes; the reverse order lets a live
		// coordinator commit into a partition that was just emptied and leave rows behind the traversal has
		// already passed. Every root and every child is wiped, not only the ones that hold rows: the
		// coordinator of a given idempotency token is not knowable from here.
		//
		// Each router owns its traversal: the fence, the target order and the dedup. FokosDB supplies the
		// stub and the destroy call.
		await this.#coordinators.walk(
			(ctx, doName) => txCoordinatorStubByName(env, ctx, doName),
			async (ctx, stub) => {
				try {
					await stub.fokosDestroy();
				} catch (e) {
					if (!isDestroyAbortError(e)) {
						throw e;
					}
				}
				console.warn(`Destroyed transaction coordinator ${ctx.doName}`);
			},
		);

		await this.#options.topology.walk(
			(ctx, doName) => partitionStubByName(env, ctx, doName),
			async (ctx, stub) => {
				try {
					await stub.fokosDestroy();
				} catch (e) {
					if (!isDestroyAbortError(e)) {
						throw e;
					}
				}
				console.warn(`Destroyed partition DO ${ctx.doName} (partitionId=${ctx.partitionId})`);
			},
		);

		return { ok: true };
	}
}
