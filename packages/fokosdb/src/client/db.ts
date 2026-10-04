import { env } from "cloudflare:workers";
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
import { partitionStubByName, txCoordinatorStubByName } from "../shared/do-stubs.js";
import { FOKOS_HASH_PARTITIONS_MAX } from "../sharding/route-context.js";
import { FokosShardingClient, dropCallCost, type FokosRetryPolicy } from "../sharding/client.js";
import type { PartitionOps } from "../server/do-partition.js";
import type { CoordinatorOps } from "../server/do-transaction-coordinator.js";
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
	TCWriteOperation,
	TransactWriteOperationResultEncoded,
	TransactionReadItem,
} from "../shared/transaction-wire-types.js";
import {
	encodeHashKey,
	encodeSortBound,
	encodeSortKey,
	validateItemDataSize,
	validateItemKeys,
	validateReturnValuesOnConditionCheckFailure,
	validateTransactGetItemCount,
	validateTransactGetItemKeys,
	validateTransactWriteOperations,
	validateClientRequestToken,
	decodeItemKeys,
	resolveLimits,
	type FokosDBLimits,
} from "../shared/transaction-limits.js";
import {
	CONFLICT_CODES,
	FokosConflictError,
	FokosError,
	FokosInternalError,
	FokosUnavailableError,
	FokosValidationError,
	CORE_INTERNAL_CODES,
	INTERNAL_CODES,
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
import {
	coordinatorShardGroup,
	createTableConfig,
	type FokosDBPolicy,
	type FokosDBTableConfig,
	type FokosTableIdentity,
	type FokosTableOptions,
} from "../shared/partition-context.js";

const TX_COORDINATORS_PER_ROOT_TREE = 2;

/** The retries of the client, for the calls that can send a request again. */
export type FokosDBRetryOptions = {
	/** The first retry waits a random time up to this value, and each later retry doubles it. Default: 100 ms. */
	baseDelayMs?: number;
	/** The longest random wait between two attempts. It must be larger than `baseDelayMs`. Default: 2,000 ms. */
	maxDelayMs?: number;
	/**
	 * The attempts of each read of a read transaction, and of the single-partition read of
	 * `transactGetItems`. A read applies nothing, so an attempt again is safe. Default: 5.
	 */
	maxAttempts?: number;
};

const DEFAULT_RETRY: Readonly<Required<FokosDBRetryOptions>> = Object.freeze({ baseDelayMs: 100, maxDelayMs: 2_000, maxAttempts: 5 });

/** The retry policy of a read: `retryable` decides which errors send the read again, up to `maxAttempts`. */
function readRetry(retry: Required<FokosDBRetryOptions>, retryable: (err: unknown) => boolean): FokosRetryPolicy {
	const { baseDelayMs, maxDelayMs, maxAttempts } = retry;
	return { shouldRetry: (err, nextAttempt) => retryable(err) && nextAttempt <= maxAttempts, baseDelayMs, maxDelayMs };
}

/** A read applies nothing, so each phase of a read transaction sends again after any error. */
const TRANSACTION_READ_RETRY = readRetry(DEFAULT_RETRY, () => true);
/** The snapshot read sends again only after a transient fault of the runtime. */
const SNAPSHOT_READ_RETRY = readRetry(DEFAULT_RETRY, (err) => isRuntimeRetryableError(err));

/** The default of `FokosDBOptions.partitionMigratingRetryDeadlineMs`. */
const PARTITION_MIGRATING_RETRY_DEADLINE_MS = 15_000;

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
		// The cost of the call is not part of the public error.
		dropCallCost(e);
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
	const mapped = new FokosUnavailableError(SHARDING_UNAVAILABLE_CODES.partition_migrating, {
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

/**
 * The options of a FokosDB client. `table` holds the identity of the table, and every client of the
 * table must give the same values. The other options can change between deploys.
 */
export type FokosDBOptions = FokosTableOptions & {
	/**
	 * Runs a transaction whose items are all owned by ONE partition against that partition directly,
	 * in a single round trip, instead of through a transaction coordinator. Defaults to true.
	 *
	 * It is an execution strategy, not a semantic: both paths give the same answer, so it belongs
	 * here and not on the per-call options. Set it to false to force every transaction through the
	 * coordinator.
	 */
	singlePartitionFastPath?: boolean;

	/**
	 * The retries of the client. The single-item operations and `queryItems` never send a request again,
	 * because a write whose answer is lost could then apply twice.
	 */
	retry?: FokosDBRetryOptions;

	/**
	 * How long `transactWriteItems` (or other write operations) send a write again while its coordinator answers
	 * `partition_migrating`, because the coordinator splits. The request carries the same token, so a
	 * retry continues the same transaction. It must stay longer than the `fallbackAlarmMs` of the runtime
	 * of the coordinators (default 5,000 ms), so that an import that a crash stopped can finish inside
	 * it. The client cannot read that setting. Default: 15,000 ms.
	 */
	partitionMigratingRetryDeadlineMs?: number;
};

/** `FokosDBOptions` with the defaults of the client applied. */
export type FokosDBResolvedOptions = Omit<
	FokosDBOptions,
	"table" | "singlePartitionFastPath" | "retry" | "partitionMigratingRetryDeadlineMs"
> & {
	table: FokosTableIdentity & { readonly coordinatorRootsN: number };
	singlePartitionFastPath: boolean;
	retry: Required<FokosDBRetryOptions>;
	partitionMigratingRetryDeadlineMs: number;
};

function validateRetryOptions(retry: Required<FokosDBRetryOptions>, partitionMigratingRetryDeadlineMs: number): void {
	const invalid = (option: string, value: number, message: string) =>
		new FokosValidationError(VALIDATION_CODES.fokosdb_options_invalid, { message, attributes: { option, value } });
	if (!Number.isSafeInteger(retry.baseDelayMs) || retry.baseDelayMs < 1) {
		throw invalid("retry.baseDelayMs", retry.baseDelayMs, "retry.baseDelayMs must be an integer of at least 1");
	}
	if (!Number.isSafeInteger(retry.maxDelayMs) || retry.maxDelayMs < 1) {
		throw invalid("retry.maxDelayMs", retry.maxDelayMs, "retry.maxDelayMs must be an integer of at least 1");
	}
	if (retry.baseDelayMs >= retry.maxDelayMs) {
		throw invalid("retry.baseDelayMs", retry.baseDelayMs, "retry.baseDelayMs must be less than retry.maxDelayMs");
	}
	if (retry.maxAttempts !== undefined && (!Number.isSafeInteger(retry.maxAttempts) || retry.maxAttempts < 1)) {
		throw invalid("retry.maxAttempts", retry.maxAttempts, "retry.maxAttempts must be an integer of at least 1");
	}
	if (!Number.isSafeInteger(partitionMigratingRetryDeadlineMs) || partitionMigratingRetryDeadlineMs < 1) {
		throw invalid(
			"partitionMigratingRetryDeadlineMs",
			partitionMigratingRetryDeadlineMs,
			"partitionMigratingRetryDeadlineMs must be an integer of at least 1",
		);
	}
}

/** The public meta of one result: the metrics of the work, and the partition that produced it. */
function publicMeta(metrics: OperationMetrics, routing: FokosPublicRouting): OperationMetrics & PartitionInfo {
	return { ...metrics, ...partitionInfoOf(routing) };
}

export class FokosDB {
	#options: FokosDBResolvedOptions;
	/** The route context parts of the table, which every coordinator request carries. */
	#table: FokosDBTableConfig;
	/** The key size limits of the table, resolved once from its policy. */
	#limits: FokosDBLimits;
	/** The partitions of the table. */
	#partitions: FokosShardingClient<FokosDBPolicy, PartitionOps>;
	/** The coordinator group of the table, `fokos.tc.<tableName>`. */
	#coordinators: FokosShardingClient<FokosDBPolicy, CoordinatorOps>;

	constructor(options: FokosDBOptions) {
		this.#table = createTableConfig(options);
		const { topology, rangeConfig, policy, policyVersion } = this.#table;
		// The default has the same maximum as the check below, so a large `rootTreesN` does not fail.
		this.#options = {
			...options,
			table: {
				...options.table,
				coordinatorRootsN:
					options.table.coordinatorRootsN ?? Math.min(TX_COORDINATORS_PER_ROOT_TREE * topology.rootTreesN, FOKOS_HASH_PARTITIONS_MAX),
			},
			singlePartitionFastPath: options.singlePartitionFastPath ?? true,
			// A client is made for each request, so the defaults are shared and not checked again.
			retry:
				options.retry === undefined
					? DEFAULT_RETRY
					: {
							baseDelayMs: options.retry.baseDelayMs ?? DEFAULT_RETRY.baseDelayMs,
							maxDelayMs: options.retry.maxDelayMs ?? DEFAULT_RETRY.maxDelayMs,
							maxAttempts: options.retry.maxAttempts ?? DEFAULT_RETRY.maxAttempts,
						},
			partitionMigratingRetryDeadlineMs: options.partitionMigratingRetryDeadlineMs ?? PARTITION_MIGRATING_RETRY_DEADLINE_MS,
		};
		if (options.retry !== undefined || options.partitionMigratingRetryDeadlineMs !== undefined) {
			validateRetryOptions(this.#options.retry, this.#options.partitionMigratingRetryDeadlineMs);
		}
		this.#limits = resolveLimits(policy.limits);
		const { coordinatorRootsN } = this.#options.table;
		if (!Number.isInteger(coordinatorRootsN) || coordinatorRootsN < 1 || coordinatorRootsN > FOKOS_HASH_PARTITIONS_MAX) {
			throw new FokosValidationError(VALIDATION_CODES.num_tx_coordinators_invalid, {
				message: `coordinatorRootsN must be an integer between 1 and ${FOKOS_HASH_PARTITIONS_MAX}`,
				attributes: { coordinatorRootsN },
			});
		}
		// The coordinators take the topology of the table except its shard group and its number of roots:
		// the same hash split fan-out, and the same jurisdiction. The range config is not used, because
		// a coordinator has no range tree.
		this.#partitions = new FokosShardingClient({
			topology,
			rangeConfig,
			policy,
			policyVersion,
			stub: (ctx, doName) => partitionStubByName(env, ctx, doName),
		});
		this.#coordinators = new FokosShardingClient({
			topology: {
				...topology,
				shardGroup: coordinatorShardGroup(topology),
				rootTreesN: coordinatorRootsN,
			},
			rangeConfig,
			policy,
			policyVersion,
			stub: (ctx, doName) => txCoordinatorStubByName(env, ctx, doName),
		});
	}

	/** The options of this client, with the defaults applied. */
	options(): FokosDBResolvedOptions {
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
		const hashKey = encodeHashKey(opts.hashKey, this.#limits);
		const sortKey = encodeSortKey(opts.sortKey, this.#limits);
		// Encode data once at this boundary; the DO receives string | Uint8Array + kind.
		const encoded = encodeItemData(opts.data);
		const condition = opts.condition ? withExpressionErrors(() => compileConditionExpression(opts.condition!)) : undefined;
		// Measured on the ENCODED form, so a json payload is capped by the text actually stored and
		// the same item is accepted or rejected identically here and in transactWriteItems.
		validateItemDataSize(encoded.data, "putItem");
		const { value: res, routing } = await this.#partitions.point(
			"apiPutItem",
			{ hashKey, sortKey },
			{
				hashKey,
				sortKey,
				data: encoded.data,
				kind: encoded.kind,
				ttlAt: opts.ttlAt,
				condition,
				returnValuesOnConditionCheckFailure: opts.returnValuesOnConditionCheckFailure,
			},
		);
		if (res.outcome === "rejected") {
			throw conditionCheckError(opts, res, routing);
		}
		// The DO returns no keys; the caller's own are the only ones it can recognise.
		return { item: { hashKey: opts.hashKey, sortKey: opts.sortKey }, version: res.version, meta: publicMeta(res.meta, routing) };
	}

	async #getItem(opts: GetItemOptions): Promise<GetItemResult> {
		validateItemKeys(opts.hashKey, opts.sortKey);
		const hashKey = encodeHashKey(opts.hashKey, this.#limits);
		const sortKey = encodeSortKey(opts.sortKey, this.#limits);
		const projection =
			opts.projection === undefined ? undefined : withExpressionErrors(() => compileProjectionExpression(opts.projection!));
		const { value: res, routing } = await this.#partitions.point(
			"apiGetItem",
			{ hashKey, sortKey },
			{ hashKey, sortKey, ...(projection === undefined ? {} : { projection }) },
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
		const hashKey = encodeHashKey(opts.hashKey, this.#limits);
		const sortKey = encodeSortKey(opts.sortKey, this.#limits);
		const condition = opts.condition ? withExpressionErrors(() => compileConditionExpression(opts.condition!)) : undefined;
		const { value: res, routing } = await this.#partitions.point(
			"apiDeleteItem",
			{ hashKey, sortKey },
			{ hashKey, sortKey, condition, returnValuesOnConditionCheckFailure: opts.returnValuesOnConditionCheckFailure },
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
		const keys = validateTransactWriteOperations(prepared, this.#limits);
		const items: TCWriteOperation[] = prepared.map((item, i) => {
			const { hashKey, sortKey } = keys[i];
			return { ...item, opIndex: i, hashKey, sortKey };
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

		// A coordinator answers `partition_migrating` while it splits: its root forwards the request to
		// the child that owns the token, and that child refuses it until its import is complete. The
		// request carries the token, so a retry resumes the same transaction and never starts a second one.
		const deadline = Date.now() + this.#options.partitionMigratingRetryDeadlineMs;
		const { baseDelayMs, maxDelayMs } = this.#options.retry;
		// The TC response carries no keys — nothing to decode at this boundary, unlike every other
		// method here. See TransactWriteItemsResult.
		const { value: encoded } = await this.#coordinators.point(
			"initiateWrite",
			{ hashKey: encodeHashKey(idempotencyToken, this.#limits), sortKey: encodeSortKey(undefined, this.#limits) },
			{ clientRequestToken: idempotencyToken, table: this.#table, items },
			{
				retry: {
					shouldRetry: (err) => FokosError.isCode(err, SHARDING_UNAVAILABLE_CODES.partition_migrating) && Date.now() < deadline,
					baseDelayMs,
					maxDelayMs,
				},
			},
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

		// A hint only: a split or a promotion below the entry can still spread the items over several
		// partitions. The partition then answers `not_applicable`.
		const groups = this.#partitions.resolveAll(items);
		if (groups.length !== 1) {
			return null;
		}

		const transactionId = crypto.randomUUID().replaceAll("-", "");
		const request = { items };

		let response: SingleShotResponse;
		try {
			// No retry, matching the coordinator path, which does not retry a write either.
			response = (await this.#partitions.send("txExecuteSingleShot", groups[0].ctx, request, items)).value;
		} catch (err) {
			// The partition does not throw after its apply commits, so an error that partition code raised
			// means nothing applied: the transaction cancelled, and that one partition owns every operation,
			// as the coordinator reports the same refusal of a prepare. A foreign error can be a reply lost
			// after the apply, so its outcome is unknown.
			if (FokosError.is(err) && !FokosError.isCode(err, CORE_INTERNAL_CODES.foreign_error)) {
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
		const items: TransactionReadItem[] = opts.items.map((item) => {
			validateItemKeys(item.hashKey, item.sortKey);
			const hashKey = encodeHashKey(item.hashKey, this.#limits);
			const sortKey = encodeSortKey(item.sortKey, this.#limits);
			const projection =
				item.projection === undefined ? undefined : withExpressionErrors(() => compileProjectionExpression(item.projection!));
			return { hashKey, sortKey, ...(projection === undefined ? {} : { projection }) };
		});
		validateTransactGetItemKeys(items);

		// TODO: Make the two-phase driver location configurable. A global caller Worker can be far from the
		// data partitions, so a coordinator near those partitions can reduce repeated cross-region trips.
		const response = (await this.#readSnapshotFastPath(items)) ?? (await this.#readTransaction(items));

		// The public boundary — the single exit where the internal representation becomes the public one:
		// decode the KeyBytes back to public keys (the empty sentinel maps to an absent sortKey, same as
		// queryItems), parse json text once into a JsonValue, and drop the read-transaction bookkeeping
		// (maxDeletedV / hasPendingWrite) so callers never depend on it. Those two are meaningless in a
		// committed read regardless — the driver raises an error when any item has a pending write.
		return {
			items: response.items.map((encoded, index) => {
				const { hasPendingWrite: _hasPendingWrite, hashKey, sortKey, ...item } = encoded;
				const keys = {
					hashKey: KeyCodec.decode(hashKey),
					sortKey: sortKey.byteLength === 0 ? undefined : KeyCodec.decode(sortKey),
				};
				if (!item.found) {
					return { ...keys, found: false as const };
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
	async #readSnapshotFastPath(items: TransactionReadItem[]): Promise<InitiateReadResponseEncoded | null> {
		if (!this.#options.singlePartitionFastPath) {
			return null;
		}
		// A hint only, as on the write fast path.
		const groups = this.#partitions.resolveAll(items);
		if (groups.length !== 1) {
			return null;
		}

		// Every error, a transport failure included, is the caller's, exactly as on the two-phase path.
		const { value: response } = await this.#partitions.send("txReadSnapshot", groups[0].ctx, { items }, items, {
			retry: this.#options.retry === DEFAULT_RETRY ? SNAPSHOT_READ_RETRY : readRetry(this.#options.retry, isRuntimeRetryableError),
		});
		// No single partition owns every key. Nothing was read, so the two-phase path runs instead.
		if (response.outcome === "not_applicable") {
			return null;
		}
		if (response.outcome === "aborted") {
			throw pendingWriteError();
		}
		return response;
	}

	async #readTransaction(requestedItems: TransactionReadItem[]): Promise<InitiateReadResponseEncoded> {
		const transactionId = crypto.randomUUID().replaceAll("-", "");
		// Both phases send the same groups.
		const groups = this.#partitions.resolveAll(requestedItems).map(({ ctx, indexes }) => {
			const items = indexes.map((i) => requestedItems[i]);
			return { ctx, items };
		});
		const retry = this.#options.retry === DEFAULT_RETRY ? TRANSACTION_READ_RETRY : readRetry(this.#options.retry, () => true);
		const readPhase = () =>
			Promise.allSettled(
				groups.map(({ ctx, items }) => this.#partitions.send("txReadForTransaction", ctx, { transactionId, items }, items, { retry })),
			);

		// Phase 1
		const phase1Settled = await readPhase();

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
		const phase2Settled = await readPhase();

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

		// Did both phases observe the same committed state? An item found in both phases compares
		// `version` (the item's `v`). The `v` of a key never repeats in a partition, also after a delete
		// and a recreate, so a different `v` shows each write between the phases. Unlike a wall-clock
		// timestamp, it cannot miss two writes in the same millisecond. This is the LSN comparison of the
		// DynamoDB paper for its read transactions. An item absent in both phases compares
		// `maxDeletedV`, the `max_deleted_v` of the owner partition: a create and a delete between the
		// phases raise it. A delete of another item can also raise it, which is a false conflict. Item
		// timestamps are not compared.
		const sameCommittedState = (a: ReadForTransactionItemResultEncoded, b: ReadForTransactionItemResultEncoded): boolean => {
			if (a.found !== b.found) {
				return false;
			}
			if (a.found && b.found) {
				return a.version === b.version;
			}
			return !a.found && !b.found && a.maxDeletedV === b.maxDeletedV;
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
				hashKey: encodeHashKey(q.hashKey, this.#limits),
				interval: normalizeSkInterval(q.sortKeyCondition, (k) => encodeSortBound(k, this.#limits)),
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

			const { value: rpcResult, routing } = await this.#partitions.range(
				"apiQueryItems",
				{ hashKey: query.hashKey, interval: query.interval, descending: query.direction === "desc" },
				{
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
				},
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
		// The client owns the traversal: the fence, the target order and the dedup.
		await this.#coordinators.destroy({ onDestroyed: (ref) => console.warn(`Destroyed transaction coordinator ${ref.doName}`) });
		await this.#partitions.destroy({
			onDestroyed: (ref) => console.warn(`Destroyed partition DO ${ref.doName} (partitionId=${ref.partitionId})`),
		});

		return { ok: true };
	}
}
