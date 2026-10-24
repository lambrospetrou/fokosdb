import { env } from "cloudflare:workers";
import {
	AnsweredPartitionMeta,
	DataKind,
	DecodedItemData,
	DeleteItemOptions,
	DeleteItemResult,
	EncodedItemData,
	FokosMeta,
	GetItemOptions,
	GetItemResult,
	HashKey,
	JsonComposite,
	JsonValue,
	PutItemOptions,
	PutItemResult,
	ProjectedItem,
	QueryItemsOptions,
	QueryItemsProjectedOptions,
	QueryItemsProjectedResult,
	QueryItemsResult,
	QuerySelect,
	SinglePartitionMeta,
	SortKey,
} from "../shared/types.js";
import { partitionStubByName, txCoordinatorStubByName } from "../shared/do-stubs.js";
import { FOKOS_HASH_PARTITIONS_MAX } from "../sharding/route-context.js";
import { FokosShardingClient, dropCallCost, type FokosCallResult, type FokosRetryPolicy } from "../sharding/client.js";
import type {
	BatchFailure,
	BatchGetItemsRpcRequest,
	BatchGetItemsRpcResponse,
	BatchWriteRpcItem,
	GetItemRpcItem,
	ItemRpcPartitions,
	PartitionOps,
} from "../server/do-partition.js";
import type { CoordinatorOps } from "../server/do-transaction-coordinator.js";
import type {
	BatchGetItemKey,
	BatchGetItemResult,
	BatchGetItemsOptions,
	BatchGetItemsResult,
	BatchUnprocessedCode,
	BatchWriteItemResult,
	BatchWriteItemsOptions,
	BatchWriteItemsResult,
} from "../shared/batch-api-types.js";
import { classifyBatchFailure, type BatchFailureOutcome } from "../shared/batch-failure.js";
import { fitBatchResponse } from "../shared/batch-response.js";
import type { FokosErrorCode } from "../shared/errors-operations.js";
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
	CoordinatorOperationResultEncoded,
	IndexedReadItemEncoded,
	InitiateReadResponseEncoded,
	ParticipantsMeta,
	ReadForTransactionItemResultEncoded,
	RejectionReasonEncoded,
	SingleShotResponse,
	TCWriteOperation,
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
	validateExecutionMode,
	validateClientRequestToken,
	validateTtlAt,
	validateBatchGetItems,
	MAX_BATCH_GET_RESPONSE_BYTES,
	validateBatchWriteItems,
	applyBatchImageCap,
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
	isFokosAnyError,
	withExpressionErrors,
	type FokosAnyError,
} from "../shared/errors-operations.js";
import invariant from "../shared/invariant.js";
import { SHARDING_UNAVAILABLE_CODES } from "../sharding/errors.js";
import { KeyCodec, KeyPairMap } from "../sharding/key-codec.js";
import { attachRouting, routedError } from "../sharding/envelope.js";
import type { FokosPublicRouting } from "../sharding/runtime-types.js";
import type { WirePartitionMeta } from "../shared/types.js";
import { normalizeSkInterval } from "../sharding/sk-interval.js";
import { answerMeta, PartitionMetaJoin, routeNodeMeta } from "../shared/partition-meta.js";
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
import { canonicalConditionIdentity, canonicalProjectionIdentity } from "../shared/expression/identity.js";
import { validateConditionExpression, validateProjectionExpression, validateUpdateExpression } from "../shared/expression/semantic.js";
import type { ConditionExpression, ProjectionExpression } from "../shared/expression/types.js";
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
	 * `transactGetItems`. A read applies nothing, so an attempt again is safe. It is also the maximum
	 * number of calls of each `FokosStd` batch retry helper. Default: 5.
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

/**
 * The error that every FokosDB method raises. `meta` names the partitions that answered the request
 * before the error, and the partition that raised it. It is absent when nothing answered: a validation
 * error before any I/O, or a failure of the RPC from the Worker to the first partition.
 */
export type FokosDBError = FokosAnyError & { readonly meta?: FokosMeta };

/** True for an error that a FokosDB method raises, after any number of hops. */
export function isFokosDBError(e: unknown): e is FokosDBError {
	return isFokosAnyError(e);
}

/**
 * Runs `fn`, which reads the answer of a partition. An error from it gets the meta of the answers, as
 * an error that a partition raised does.
 */
function withAnswerMeta<T>(meta: FokosMeta, fn: () => T): T {
	try {
		return fn();
	} catch (e) {
		throw Object.assign(FokosError.wrap(e), { meta });
	}
}

/** The `meta` of a method that one partition ran, from the entry of its answer. */
function singleMeta(partitions: ItemRpcPartitions, routing: FokosPublicRouting): SinglePartitionMeta {
	const {
		forwardCount,
		partitions: [entry],
	} = answerMeta(partitions, routing);
	return { forwardCount, partitions: [entry] };
}

/** The meta of no answer. */
const NO_META: FokosMeta = { forwardCount: 0, partitions: [] };

/** `soFar` with the entry of the partition that raised an error, which `RouteCollector.build` keeps at the head. */
function withRaiser(soFar: FokosMeta, routing: FokosPublicRouting): FokosMeta {
	const raiser = routing.servedBy[0];
	return {
		forwardCount: soFar.forwardCount + routing.forwardCount,
		partitions: raiser ? [...soFar.partitions, routeNodeMeta(raiser)] : soFar.partitions,
	};
}

/**
 * Raises `e` as a public FokosError with the `meta` of the answers that the client has, and the entry
 * of the partition that raised `e` when it carries routing. The routing state stops here, as it does
 * on a result. A FokosError that has its `meta` already stays as it is.
 */
function withMetaSoFar(e: unknown, soFar: FokosMeta): FokosError {
	// The cost of the call is not part of the public error.
	dropCallCost(e);
	const err = mapInternalErrorToPublic(FokosError.wrap(e));
	const routed = routedError(err);
	if (!routed) {
		return soFar.partitions.length > 0 ? Object.assign(err, { meta: soFar }) : err;
	}
	const { routing } = routed;
	delete (routed as { routing?: unknown }).routing;
	return Object.assign(err, { meta: withRaiser(soFar, routing) });
}

/**
 * Gives an error of the coordinator path its `meta`. The coordinator sends the entries of the
 * participant answers of the request as the own `meta` of the error, and the routing names the
 * coordinator that raised it.
 */
function coordinatorError(e: unknown): FokosError {
	dropCallCost(e);
	const err = mapInternalErrorToPublic(FokosError.wrap(e));
	const routed = routedError(err);
	if (!routed) {
		return err;
	}
	const { routing } = routed;
	delete (routed as { routing?: unknown }).routing;
	const participants = (err as { meta?: ParticipantsMeta }).meta ?? NO_META;
	const raiser = routing.servedBy[0];
	return Object.assign(err, { meta: { ...participants, ...(raiser ? { txCoordinator: routeNodeMeta(raiser) } : {}) } satisfies FokosMeta });
}

/** The error `putItem` and `deleteItem` raise when the partition rejects the condition. */
function conditionCheckError(
	keys: { hashKey: HashKey; sortKey?: SortKey },
	res: { reason: RejectionReasonEncoded },
	meta: FokosMeta<AnsweredPartitionMeta>,
): FokosConditionCheckError {
	const reason = withAnswerMeta(meta, () => decodeRejectionReason(res.reason));
	invariant(reason.code === "condition_failed", "an item RPC rejects only a failed condition");
	return new FokosConditionCheckError(CONDITION_CHECK_CODES.condition_failed, {
		message: "condition failed",
		attributes: { hashKey: keys.hashKey, sortKey: keys.sortKey },
		reason,
		meta,
	});
}

/** The error `transactWriteItems` raises for a cancelled transaction, on either path. */
function transactionCancelledError(fields: {
	transactionId: string;
	idempotencyToken: string;
	results: CoordinatorOperationResultEncoded[];
	meta: FokosMeta;
}): FokosTransactionCancelledError {
	return new FokosTransactionCancelledError(TRANSACTION_CANCELLED_CODES.transaction_cancelled, {
		message: "transaction cancelled",
		attributes: { transactionId: fields.transactionId, idempotencyToken: fields.idempotencyToken },
		results: withAnswerMeta(fields.meta, () => fields.results.map(decodeOperationResult)),
		meta: fields.meta,
	});
}

function decodeOperationResult(res: CoordinatorOperationResultEncoded): TransactWriteOperationResult {
	if (res.outcome !== "rejected") {
		return { outcome: res.outcome };
	}
	return {
		outcome: "rejected",
		reason: decodeRejectionReason(res.reason),
		...(res.partitionIndex === undefined ? {} : { partitionIndex: res.partitionIndex }),
		...(res.itemOmitted ? { itemOmitted: res.itemOmitted } : {}),
	};
}

/** `transactGetItems` raises it when a requested item holds a pending write of an in-progress transaction. */
function pendingWriteError(meta: FokosMeta): FokosConflictError {
	return Object.assign(
		new FokosConflictError(CONFLICT_CODES.pending_write, { message: "an item has a pending write of an in-progress transaction" }),
		{ meta },
	);
}

/** The fields of the result of an item that one failure stopped. */
type BatchFailureFields = { code: BatchUnprocessedCode; error_id?: string; partitionIndex?: number };

/**
 * Joins the answers of the calls of a batch, in call order, into one `meta`. `groups[i]` holds the
 * items of call `i`. A call can go to any partition of the table, and that partition forwards the
 * items that it does not own. Each answer adds its entries to the joined list, and `answer` gets the
 * function that moves a `partitionIndex` of that answer to the joined list. `failure` gets each item
 * that a failure stopped: a failure in an answer, or a call that failed, which stops each item of that
 * call. A failed call adds the entry of the partition that raised its error, when the error carries
 * routing.
 */
function joinBatchAnswers<R extends { failed: BatchFailure[]; partitions: WirePartitionMeta[] }>(
	settled: ReadonlyArray<PromiseSettledResult<FokosCallResult<R>>>,
	groups: ReadonlyArray<{ indexes: number[] }>,
	write: boolean,
	on: {
		answer(value: R, at: (partitionIndex: number) => number): void;
		/** `entry` is the failure of an answer. It is absent for a call that failed. */
		failure(opIndex: number, failure: { outcome: BatchFailureOutcome; fields: BatchFailureFields }, entry?: BatchFailure): void;
	},
): FokosMeta {
	const join = new PartitionMetaJoin();
	let forwardCount = 0;
	settled.forEach((r, g) => {
		if (r.status === "fulfilled") {
			const { value, routing } = r.value;
			const answer = answerMeta(value.partitions, routing);
			forwardCount += answer.forwardCount;
			const indexes = answer.partitions.map((entry) => join.add(entry));
			const at = (partitionIndex: number) => indexes[partitionIndex];
			on.answer(value, at);
			for (const entry of value.failed) {
				const { outcome, code, error_id, partitionIndex, opIndexes } = entry;
				const fields: BatchFailureFields = {
					code: publicBatchCode(code),
					...(error_id === undefined ? {} : { error_id }),
					...(partitionIndex === undefined ? {} : { partitionIndex: at(partitionIndex) }),
				};
				for (const opIndex of opIndexes) {
					on.failure(opIndex, { outcome, fields }, entry);
				}
			}
			return;
		}
		const { outcome, code, error_id } = classifyBatchFailure(r.reason, { write, local: false });
		const routed = routedError(r.reason);
		let partitionIndex: number | undefined;
		if (routed) {
			forwardCount += routed.routing.forwardCount;
			const raiser = routed.routing.servedBy[0];
			partitionIndex = raiser ? join.add(routeNodeMeta(raiser)) : undefined;
		}
		const fields: BatchFailureFields = {
			code: publicBatchCode(code),
			error_id,
			...(partitionIndex === undefined ? {} : { partitionIndex }),
		};
		for (const opIndex of groups[g].indexes) {
			on.failure(opIndex, { outcome, fields });
		}
	});
	return { forwardCount, partitions: join.partitions };
}

/** The public code of a batch result, mapped as `mapInternalErrorToPublic` maps the code of an error. */
function publicBatchCode(code: BatchUnprocessedCode): BatchUnprocessedCode {
	return code === SHARDING_UNAVAILABLE_CODES.repartition_not_cut_over.code ? SHARDING_UNAVAILABLE_CODES.partition_migrating.code : code;
}

/** The `error_id` of a defect in a batch answer, which is logged here. The method still answers each item. */
function defectErrorId(message: string, attributes: Record<string, unknown> = {}): string {
	const err = new FokosInternalError(CORE_INTERNAL_CODES.invariant_failed, { message, attributes });
	console.error({ message: `fokos/batch: ${message}`, error_id: err.error_id, ...attributes });
	return err.error_id;
}

/**
 * The public result of one found item of a batch read, with the caller's own keys. An error while it
 * decodes the item makes the item `unprocessed`, because the method never throws after I/O starts.
 */
function decodeFoundItem(
	item: GetItemRpcItem,
	names: readonly string[] | undefined,
	keys: { hashKey: HashKey; sortKey?: SortKey },
	partitionIndex: number,
): BatchGetItemResult {
	try {
		if (item.kind === "projected") {
			invariant(names, "fokos/batchGetItems: projected result without a projection");
			return {
				outcome: "found",
				partitionIndex,
				...keys,
				data: projectedItemFromWireRow(names, item.projected),
				kind: "projected",
				...(item.ttlAt === undefined ? {} : { ttlAt: item.ttlAt }),
				version: item.version,
			};
		}
		return { outcome: "found", partitionIndex, ...keys, ...item, ...decodeItemData(item.kind, item.data) };
	} catch (e) {
		const err = FokosError.wrap(e);
		return { outcome: "unprocessed", code: err.code as FokosErrorCode, error_id: err.error_id, partitionIndex, ...keys };
	}
}

/** Runs the body of a public method, and raises any error from it as a FokosError. */
async function withFokosErrors<T>(fn: () => Promise<T>): Promise<T> {
	try {
		return await fn();
	} catch (e) {
		// A partition attaches its routing to its error, and the public error names that partition. A
		// method that has more answers gave the error its `meta` and dropped the routing already.
		throw withMetaSoFar(e, NO_META);
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

/** The retry options with the defaults applied, validated. It throws `fokosdb_options_invalid` on a value that is not valid. */
export function resolveRetryOptions(retry: FokosDBRetryOptions | undefined): Required<FokosDBRetryOptions> {
	if (retry === undefined) {
		return DEFAULT_RETRY;
	}
	const resolved = {
		baseDelayMs: retry.baseDelayMs ?? DEFAULT_RETRY.baseDelayMs,
		maxDelayMs: retry.maxDelayMs ?? DEFAULT_RETRY.maxDelayMs,
		maxAttempts: retry.maxAttempts ?? DEFAULT_RETRY.maxAttempts,
	};
	validateRetryOptions(resolved);
	return resolved;
}

const invalidOption = (option: string, value: number, message: string) =>
	new FokosValidationError(VALIDATION_CODES.fokosdb_options_invalid, { message, attributes: { option, value } });

function validateRetryOptions(retry: Required<FokosDBRetryOptions>): void {
	if (!Number.isSafeInteger(retry.baseDelayMs) || retry.baseDelayMs < 1) {
		throw invalidOption("retry.baseDelayMs", retry.baseDelayMs, "retry.baseDelayMs must be an integer of at least 1");
	}
	if (!Number.isSafeInteger(retry.maxDelayMs) || retry.maxDelayMs < 1) {
		throw invalidOption("retry.maxDelayMs", retry.maxDelayMs, "retry.maxDelayMs must be an integer of at least 1");
	}
	if (retry.baseDelayMs >= retry.maxDelayMs) {
		throw invalidOption("retry.baseDelayMs", retry.baseDelayMs, "retry.baseDelayMs must be less than retry.maxDelayMs");
	}
	if (retry.maxAttempts !== undefined && (!Number.isSafeInteger(retry.maxAttempts) || retry.maxAttempts < 1)) {
		throw invalidOption("retry.maxAttempts", retry.maxAttempts, "retry.maxAttempts must be an integer of at least 1");
	}
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
			retry: resolveRetryOptions(options.retry),
			partitionMigratingRetryDeadlineMs: options.partitionMigratingRetryDeadlineMs ?? PARTITION_MIGRATING_RETRY_DEADLINE_MS,
		};
		const deadlineMs = this.#options.partitionMigratingRetryDeadlineMs;
		if (!Number.isSafeInteger(deadlineMs) || deadlineMs < 1) {
			throw invalidOption(
				"partitionMigratingRetryDeadlineMs",
				deadlineMs,
				"partitionMigratingRetryDeadlineMs must be an integer of at least 1",
			);
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

	/**
	 * Reads 0 to 100 items by key, and answers each key in request order. It is not a snapshot across
	 * partitions, and it does not retry: send `unprocessedKeys` again, with the `projection` and the
	 * `maxResponseBytes` of the request, or use `FokosStd.batchGetItemsWithRetry`. Only a request that is
	 * not valid throws.
	 * `Ts` types each item by position, as `transactGetItems` does. The library cannot check `Ts`: it is the
	 * caller's own statement about what each item holds.
	 */
	async batchGetItems<Ts extends readonly unknown[] = never[]>(opts: NoInfer<BatchGetItemsOptions<Ts>>): Promise<BatchGetItemsResult<Ts>> {
		return (await withFokosErrors(async () => await this.#batchGetItems(opts))) as BatchGetItemsResult<Ts>;
	}

	/**
	 * Puts or deletes 0 to 100 items, and answers each operation in request order. It is not atomic, and
	 * it does not retry: send `unprocessedItems` again, or use `FokosStd.batchWriteItemsWithRetry`. Only a
	 * request that is not valid throws, because after the first RPC an error for the whole call could
	 * not say which items applied.
	 */
	async batchWriteItems(opts: BatchWriteItemsOptions): Promise<BatchWriteItemsResult> {
		return await withFokosErrors(async () => await this.#batchWriteItems(opts));
	}

	/** Stops at the first failure. A partial destroy stays partial, and a later call continues it. */
	async destroy(): Promise<{ ok: true }> {
		return await withFokosErrors(async () => await this.#destroy());
	}

	/**
	 * Validates a condition, so that a condition that is not valid fails before any I/O. The client
	 * never compiles: the partition compiles the expression tree, and only the partition checks the
	 * limits of the compiled SQL.
	 */
	#checkCondition(condition: ConditionExpression | undefined): void {
		if (condition !== undefined) {
			withExpressionErrors(() => validateConditionExpression(condition));
		}
	}

	async #putItem(opts: PutItemOptions): Promise<PutItemResult> {
		validateTtlAt(opts.ttlAt, "putItem");
		validateItemKeys(opts.hashKey, opts.sortKey);
		validateReturnValuesOnConditionCheckFailure(opts.returnValuesOnConditionCheckFailure);
		const hashKey = encodeHashKey(opts.hashKey, this.#limits);
		const sortKey = encodeSortKey(opts.sortKey, this.#limits);
		// Encode data once at this boundary; the DO receives string | Uint8Array + kind.
		const encoded = encodeItemData(opts.data);
		this.#checkCondition(opts.condition);
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
				condition: opts.condition,
				returnValuesOnConditionCheckFailure: opts.returnValuesOnConditionCheckFailure,
			},
		);
		const meta = singleMeta(res.partitions, routing);
		if (res.outcome === "rejected") {
			throw conditionCheckError(opts, res, meta);
		}
		// The DO returns no keys; the caller's own are the only ones it can recognise.
		return { item: { hashKey: opts.hashKey, sortKey: opts.sortKey }, version: res.version, meta };
	}

	async #getItem(opts: GetItemOptions): Promise<GetItemResult> {
		validateItemKeys(opts.hashKey, opts.sortKey);
		const hashKey = encodeHashKey(opts.hashKey, this.#limits);
		const sortKey = encodeSortKey(opts.sortKey, this.#limits);
		const { projection } = opts;
		const names = projection === undefined ? undefined : withExpressionErrors(() => validateProjectionExpression(projection)).names;
		const { value: res, routing } = await this.#partitions.point(
			"apiGetItem",
			{ hashKey, sortKey },
			{ hashKey, sortKey, ...(projection === undefined ? {} : { projection }) },
		);
		const meta = singleMeta(res.partitions, routing);
		// The DO returns no keys; supply the caller's own and preserve the found/not-found discriminant.
		// json data arrives as JSON text — parse it once here to the public JsonValue.
		return withAnswerMeta(meta, (): GetItemResult => {
			if (!res.found) {
				return { found: false, item: { hashKey: opts.hashKey, sortKey: opts.sortKey }, meta };
			}
			if (res.item.kind === "projected") {
				invariant(names, "fokos/getItem: projected response without a projection");
				return {
					found: true,
					item: {
						hashKey: opts.hashKey,
						sortKey: opts.sortKey,
						data: projectedItemFromWireRow(names, res.item.projected),
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
		});
	}

	async #deleteItem(opts: DeleteItemOptions): Promise<DeleteItemResult> {
		validateItemKeys(opts.hashKey, opts.sortKey);
		validateReturnValuesOnConditionCheckFailure(opts.returnValuesOnConditionCheckFailure);
		const hashKey = encodeHashKey(opts.hashKey, this.#limits);
		const sortKey = encodeSortKey(opts.sortKey, this.#limits);
		this.#checkCondition(opts.condition);
		const { value: res, routing } = await this.#partitions.point(
			"apiDeleteItem",
			{ hashKey, sortKey },
			{ hashKey, sortKey, condition: opts.condition, returnValuesOnConditionCheckFailure: opts.returnValuesOnConditionCheckFailure },
		);
		const meta = singleMeta(res.partitions, routing);
		if (res.outcome === "rejected") {
			throw conditionCheckError(opts, res, meta);
		}
		// The DO returns no keys; the caller's own are the only ones it can recognise.
		return { item: { hashKey: opts.hashKey, sortKey: opts.sortKey }, deleted: res.deleted, meta };
	}

	async #transactWriteItems(opts: TransactWriteItemsOptions): Promise<TransactWriteItemsResult> {
		const executionMode = validateExecutionMode(opts.executionMode);
		if (opts.clientRequestToken !== undefined) {
			validateClientRequestToken(opts.clientRequestToken);
		}

		// Encode each put, and check each condition and each update, once at this boundary. Each operation
		// keeps its expression trees. A `data` field set on a non-put by a non-TypeScript caller stays
		// present so validation rejects it.
		const prepared = opts.items.map((item) => {
			this.#checkCondition(item.condition);
			if (item.operation === "update") {
				validateTtlAt(item.ttlAt, "transactWriteItems");
				withExpressionErrors(() => validateUpdateExpression(item.update));
				return item;
			}
			if (item.operation !== "put") {
				return item;
			}
			validateTtlAt(item.ttlAt, "transactWriteItems");
			return { ...item, ...encodeItemData(item.data) };
		});
		// Validation encodes each key exactly once and hands the canonical bytes back in input order.
		const keys = validateTransactWriteOperations(prepared, this.#limits, executionMode);
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
		let answer: FokosCallResult<CoordinatorOps["initiateWrite"]["res"]>;
		try {
			answer = await this.#coordinators.point(
				"initiateWrite",
				{ hashKey: encodeHashKey(idempotencyToken, this.#limits), sortKey: encodeSortKey(undefined, this.#limits) },
				{ clientRequestToken: idempotencyToken, table: this.#table, items, executionMode },
				{
					retry: {
						shouldRetry: (err) => FokosError.isCode(err, SHARDING_UNAVAILABLE_CODES.partition_migrating) && Date.now() < deadline,
						baseDelayMs,
						maxDelayMs,
					},
				},
			);
		} catch (e) {
			throw coordinatorError(e);
		}
		const { value: encoded, routing } = answer;
		// The participant answers of this request, and the coordinator that answered it.
		const coordinator = routing.servedBy.find((node) => node.role === "executed") ?? routing.servedBy[0];
		const meta: FokosMeta = { ...encoded.meta, ...(coordinator ? { txCoordinator: routeNodeMeta(coordinator) } : {}) };
		// The outcome is the driver's, not the caller's: a committed transaction is the only value this
		// method returns, and a cancelled one raises instead.
		if (encoded.outcome === "committed") {
			return { transactionId: encoded.transactionId, idempotencyToken: encoded.idempotencyToken, meta };
		}
		throw transactionCancelledError({ ...encoded, meta });
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

		let answer: FokosCallResult<SingleShotResponse>;
		try {
			// No retry, matching the coordinator path, which does not retry a write either.
			answer = await this.#partitions.send("txExecuteSingleShot", groups[0].ctx, request, items);
		} catch (err) {
			// The partition does not throw after its apply commits, so an error that partition code raised
			// means nothing applied: the transaction cancelled, and that one partition owns every operation,
			// as the coordinator reports the same refusal of a prepare. A foreign error can be a reply lost
			// after the apply, so its outcome is unknown. The error has no answer list, so its one entry is
			// the partition that raised it, and every operation names that entry.
			const routed = routedError(err);
			if (routed && !FokosError.isCode(routed, CORE_INTERNAL_CODES.foreign_error)) {
				dropCallCost(routed);
				throw transactionCancelledError({
					transactionId,
					idempotencyToken: transactionId,
					results: items.map((item) => ({
						outcome: "rejected",
						reason: { code: routed.code as ExecutionFailureCode, ...decodeItemKeys(item.hashKey, item.sortKey), error_id: routed.error_id },
						partitionIndex: 0,
					})),
					meta: withRaiser(NO_META, routed.routing),
				});
			}
			throw err;
		}

		const { value: response } = answer;
		// No single partition owns every item. Nothing was written, so the coordinator path runs instead.
		if (response.outcome === "not_applicable") {
			return null;
		}
		const meta = answerMeta(response.partitions, answer.routing);
		if (response.outcome === "committed") {
			return { transactionId, idempotencyToken: transactionId, meta };
		}
		// The one partition of the answer rejected each rejected operation.
		const results = response.results.map((r) => (r.outcome === "rejected" ? { ...r, partitionIndex: 0 } : r));
		throw transactionCancelledError({ transactionId, idempotencyToken: transactionId, results, meta });
	}

	// The positional types of the public method are erased here: every item takes the same path, and only
	// the caller knows which type belongs to which position.
	async #transactGetItems(opts: { items: readonly TransactGetItemKey[] }): Promise<TransactGetItemsResult> {
		validateTransactGetItemCount(opts.items.length);
		// Each item is built explicitly, and it carries its projection only when the caller asked for one.
		// `names[i]` holds the output names of the projection of item `i`.
		const names: (readonly string[] | undefined)[] = [];
		const items: TransactionReadItem[] = opts.items.map((item) => {
			validateItemKeys(item.hashKey, item.sortKey);
			const hashKey = encodeHashKey(item.hashKey, this.#limits);
			const sortKey = encodeSortKey(item.sortKey, this.#limits);
			const { projection } = item;
			names.push(projection === undefined ? undefined : withExpressionErrors(() => validateProjectionExpression(projection)).names);
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
		return withAnswerMeta(response.meta, () => ({
			items: response.items.map((encoded, index) => {
				const { hasPendingWrite: _hasPendingWrite, partitionIndex, hashKey, sortKey, ...item } = encoded;
				const keys = {
					hashKey: KeyCodec.decode(hashKey),
					sortKey: sortKey.byteLength === 0 ? undefined : KeyCodec.decode(sortKey),
				};
				if (!item.found) {
					return { ...keys, found: false as const, partitionIndex };
				}
				if (item.kind === "projected") {
					// items[i] answers request.items[i], so the names of the record come from the projection of that item.
					const itemNames = names[index];
					invariant(itemNames, "fokos/transactGetItems: projected result without a projection");
					return {
						...keys,
						found: true as const,
						data: projectedItemFromWireRow(itemNames, item.projected),
						kind: "projected" as const,
						version: item.version,
						...(item.ttlAt === undefined ? {} : { ttlAt: item.ttlAt }),
						partitionIndex,
					};
				}
				return { ...keys, ...item, ...decodeItemData(item.kind, item.data), partitionIndex };
			}),
			meta: response.meta,
		}));
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
		const { value: response, routing } = await this.#partitions.send("txReadSnapshot", groups[0].ctx, { items }, items, {
			retry: this.#options.retry === DEFAULT_RETRY ? SNAPSHOT_READ_RETRY : readRetry(this.#options.retry, isRuntimeRetryableError),
		});
		// No single partition owns every key. Nothing was read, so the two-phase path runs instead.
		if (response.outcome === "not_applicable") {
			return null;
		}
		const meta = answerMeta(response.partitions, routing);
		if (response.outcome === "aborted") {
			throw pendingWriteError(meta);
		}
		// One partition answers for every key, so its list of one entry needs no join, and the items are
		// positional to the request.
		return { outcome: "committed", items: response.items.map((item) => ({ ...item, partitionIndex: 0 })), meta };
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

		// The entries of every answer of both phases, one for each partition. Each item changes its index
		// from the list of its answer to the joined list.
		const join = new PartitionMetaJoin();
		let forwardCount = 0;
		const meta = (): FokosMeta => ({ forwardCount, partitions: join.partitions });
		// A read applies nothing, so the error of a failed phase call is the answer, as the partition raised
		// it. The error has the entries of every answer so far.
		const settlePhase = (settled: Awaited<ReturnType<typeof readPhase>>): IndexedReadItemEncoded[] => {
			const flat: IndexedReadItemEncoded[] = [];
			let failed: PromiseRejectedResult | undefined;
			for (const r of settled) {
				if (r.status === "rejected") {
					failed ??= r;
					continue;
				}
				const { value, routing } = r.value;
				const answer = answerMeta(value.partitions, routing);
				forwardCount += answer.forwardCount;
				const indexes = answer.partitions.map((entry) => join.add(entry));
				flat.push(...value.items.map((item) => ({ ...item, partitionIndex: indexes[item.partitionIndex] })));
			}
			if (failed) {
				throw withMetaSoFar(failed.reason, meta());
			}
			if (flat.some((item) => item.hasPendingWrite)) {
				throw pendingWriteError(meta());
			}
			return flat;
		};

		// Phase 1
		const phase1Flat = settlePhase(await readPhase());

		// Phase 2 — verify no concurrent mutations
		const phase2Flat = settlePhase(await readPhase());

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
		// request order is restored here, once.
		//
		// Pair the two phases by key, not by position: PartitionDO fans items out to child partitions and
		// flattens the replies, so result order is not request order. KeyPairMap is the ONE identity
		// primitive for a (hashKey, sortKey) pair — the same one commitLocal's keyset check uses. It
		// compares the key bytes.
		const phase1ByKey = new KeyPairMap<IndexedReadItemEncoded>();
		for (const r of phase1Flat) {
			phase1ByKey.set(r.hashKey, r.sortKey, r);
		}
		const phase2ByKey = new KeyPairMap<ReadForTransactionItemResultEncoded>();
		for (const r of phase2Flat) {
			phase2ByKey.set(r.hashKey, r.sortKey, r);
		}
		const items: IndexedReadItemEncoded[] = [];
		for (const requested of requestedItems) {
			const p1 = phase1ByKey.get(requested.hashKey, requested.sortKey);
			const p2 = phase2ByKey.get(requested.hashKey, requested.sortKey);
			// A requested key with no reply means a participant dropped it — never expected, and not
			// something to answer with a short array.
			invariant(p1 && p2, "a participant of a read transaction dropped a requested key");
			if (!sameCommittedState(p1, p2)) {
				throw Object.assign(
					new FokosConflictError(CONFLICT_CODES.read_conflict, {
						message: "a write changed an item between the two phases of the read",
						attributes: decodeItemKeys(requested.hashKey, requested.sortKey),
					}),
					{ meta: meta() },
				);
			}
			items.push(p1);
		}
		return { outcome: "committed", items, meta: meta() };
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
		// The partition gets the expression trees and compiles them. The client validates them, which
		// gives the names of the projection, and computes their identities for the cursor.
		const { filter, projection } = opts;
		const expressions = filter === undefined && projection === undefined ? null : { filter, projection };
		const { filterIdentity, projectionIdentity, projectionNames } = withExpressionErrors(() => {
			if (filter !== undefined) {
				validateConditionExpression(filter, "filter");
			}
			return {
				filterIdentity: filter === undefined ? null : canonicalConditionIdentity(filter),
				projectionNames: projection === undefined ? null : validateProjectionExpression(projection).names,
				projectionIdentity: projection === undefined ? null : canonicalProjectionIdentity(projection),
			};
		});

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
		const fingerprint = computeCursorFingerprint(normalizedQueries, filterIdentity, projectionIdentity);

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
		// Each leaf writes its own entry, so the cap of the route list cannot drop a leaf. Only a name can
		// be absent. The entries of the sub-queries follow in order, so the items stay in entry order.
		const partitions: QueryItemsResult["meta"]["partitions"] = [];
		let count = 0;
		let scannedCount = 0;
		let rowsReturned = 0;
		let forwardCount = 0;
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

			let answer: FokosCallResult<PartitionOps["apiQueryItems"]["res"]>;
			try {
				answer = await this.#partitions.range(
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
						plan: expressions,
					},
				);
			} catch (e) {
				throw withMetaSoFar(e, { forwardCount, partitions });
			}
			const { value: rpcResult, routing } = answer;
			partitions.push(...answerMeta(rpcResult.partitions, routing).partitions);
			forwardCount += routing.forwardCount;

			count += rpcResult.count;
			scannedCount += rpcResult.scannedCount;
			rowsReturned += rpcResult.rowsReturned;
			if (select === "projection") {
				withAnswerMeta({ forwardCount, partitions }, () => {
					if (projectionNames) {
						for (const item of rpcResult.items) {
							items.push(projectedItemFromWireRow(projectionNames, item as ProjectedWireRow));
						}
					} else {
						for (const item of rpcResult.items) {
							const stored = item as StoredItem;
							items.push({
								hashKey: KeyCodec.decode(stored.hk),
								sortKey: stored.sk.byteLength === 0 ? undefined : KeyCodec.decode(stored.sk),
								// json data arrives as JSON text and is parsed once here to the public JsonValue.
								...decodeItemData(stored.kind, stored.data),
								...(stored.ttl_epoch_utc_seconds === null ? {} : { ttlAt: stored.ttl_epoch_utc_seconds }),
								version: stored.v,
							});
						}
					}
				});
			}
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

		const meta: QueryItemsResult["meta"] = { forwardCount, partitions, rowsReturned };

		return { items, count, scannedCount, cursor, meta } as QueryItemsResult | QueryItemsProjectedResult;
	}

	// The positional types of the public method are erased here, as in `#transactGetItems`.
	async #batchGetItems(opts: {
		items: readonly BatchGetItemKey[];
		projection?: readonly ProjectionExpression[];
		maxResponseBytes?: number;
	}): Promise<BatchGetItemsResult> {
		const keys = validateBatchGetItems(opts.items, this.#limits);
		// `names[i]` holds the output names of the projection of item `i`: its own, or the projection of the request.
		const namesOf = (projection: readonly ProjectionExpression[] | undefined) =>
			projection === undefined ? undefined : withExpressionErrors(() => validateProjectionExpression(projection)).names;
		const requestNames = namesOf(opts.projection);
		const names = opts.items.map((item) => (item.projection === undefined ? requestNames : namesOf(item.projection)));
		if (opts.maxResponseBytes !== undefined && (!Number.isSafeInteger(opts.maxResponseBytes) || opts.maxResponseBytes <= 0)) {
			throw new FokosValidationError(VALIDATION_CODES.batch_max_response_bytes_invalid, {
				message: "maxResponseBytes must be a positive integer when provided",
				attributes: { maxResponseBytes: opts.maxResponseBytes },
			});
		}
		// `fitBatchResponse` clamps the value on each node.
		const maxResponseBytes = opts.maxResponseBytes ?? MAX_BATCH_GET_RESPONSE_BYTES;
		if (opts.items.length === 0) {
			return { items: [], unprocessedKeys: [], meta: { forwardCount: 0, partitions: [] } };
		}
		const items: BatchGetItemsRpcRequest["items"] = opts.items.map((item, opIndex) => ({
			opIndex,
			...keys[opIndex],
			...(item.projection === undefined ? {} : { projection: item.projection }),
		}));

		const groups = this.#partitions.resolveAll(items);
		const settled = await Promise.allSettled(
			groups.map(({ ctx, indexes }) => {
				const part = indexes.map((i) => items[i]);
				const request = { items: part, ...(opts.projection === undefined ? {} : { projection: opts.projection }), maxResponseBytes };
				return this.#partitions.send("apiBatchGetItems", ctx, request, part);
			}),
		);

		const keysOf = (i: number) => ({ hashKey: opts.items[i].hashKey, sortKey: opts.items[i].sortKey });
		const results: Array<BatchGetItemResult | undefined> = Array.from({ length: items.length });
		const found: BatchGetItemsRpcResponse["found"] = [];
		const meta = joinBatchAnswers(settled, groups, false, {
			answer: (value, at) => {
				for (const entry of value.found) {
					found.push({ ...entry, partitionIndex: at(entry.partitionIndex) });
				}
				for (const { opIndex, partitionIndex } of value.notFound) {
					results[opIndex] = { outcome: "not_found", partitionIndex: at(partitionIndex), ...keysOf(opIndex) };
				}
			},
			failure: (opIndex, f) => {
				results[opIndex] =
					f.outcome === "rejected"
						? {
								outcome: "rejected",
								...f.fields,
								code: f.fields.code as FokosErrorCode,
								error_id: f.fields.error_id ?? defectErrorId("a rejected read has no error_id", { opIndex }),
								...keysOf(opIndex),
							}
						: { outcome: "unprocessed", ...f.fields, ...keysOf(opIndex) };
			},
		});
		// Each answer fits in the limit, but the answers of several calls together can go above it.
		const { kept, dropped } = fitBatchResponse(found, maxResponseBytes);
		for (const { opIndex, item, partitionIndex } of kept) {
			results[opIndex] = decodeFoundItem(item, names[opIndex], keysOf(opIndex), partitionIndex);
		}
		for (const { opIndex, partitionIndex } of dropped) {
			results[opIndex] = { outcome: "unprocessed", code: "response_too_large", partitionIndex, ...keysOf(opIndex) };
		}
		const out = results.map(
			(result, opIndex): BatchGetItemResult =>
				result ?? {
					outcome: "unprocessed",
					code: CORE_INTERNAL_CODES.invariant_failed.code,
					error_id: defectErrorId("a batch read answer has no outcome for an item", { opIndex }),
					...keysOf(opIndex),
				},
		);
		return { items: out, unprocessedKeys: opts.items.filter((_, i) => out[i].outcome === "unprocessed"), meta };
	}

	async #batchWriteItems(opts: BatchWriteItemsOptions): Promise<BatchWriteItemsResult> {
		// The fields of each operation are checked before its data is encoded, so a put with no data
		// fails with `batch_operation_fields_invalid`.
		const keys = validateBatchWriteItems(opts.items, this.#limits);
		if (opts.items.length === 0) {
			return { results: [], unprocessedItems: [], meta: { forwardCount: 0, partitions: [] } };
		}
		const items: BatchWriteRpcItem[] = opts.items.map((item, opIndex) => {
			this.#checkCondition(item.condition);
			const condition = {
				...(item.condition === undefined ? {} : { condition: item.condition }),
				...(item.returnValuesOnConditionCheckFailure === undefined
					? {}
					: { returnValuesOnConditionCheckFailure: item.returnValuesOnConditionCheckFailure }),
			};
			if (item.operation === "delete") {
				return { opIndex, ...keys[opIndex], operation: "delete", ...condition };
			}
			const encoded = encodeItemData(item.data);
			validateItemDataSize(encoded.data, "batchWriteItems");
			const ttlAt = item.ttlAt === undefined ? {} : { ttlAt: item.ttlAt };
			return { opIndex, ...keys[opIndex], operation: "put", ...encoded, ...ttlAt, ...condition };
		});

		// No retry: a write whose answer is lost could then apply twice.
		const groups = this.#partitions.resolveAll(items);
		const settled = await Promise.allSettled(
			groups.map(({ ctx, indexes }) => {
				const part = indexes.map((i) => items[i]);
				return this.#partitions.send("apiBatchWriteItems", ctx, { items: part }, part);
			}),
		);

		// Each root caps the images of its own answer, so the answers together can still be above the cap.
		applyBatchImageCap(settled.flatMap((r) => (r.status === "fulfilled" ? r.value.value.failed : [])));
		const results: Array<BatchWriteItemResult | undefined> = Array.from({ length: items.length });
		const conditionFailures: Array<{ opIndex: number; reason: RejectionReasonEncoded; itemOmitted?: "response_too_large" }> = [];
		const meta = joinBatchAnswers(settled, groups, true, {
			answer: (value, at) => {
				for (const { opIndex, version, partitionIndex } of value.appliedPuts) {
					results[opIndex] = { outcome: "ok", operation: "put", version, partitionIndex: at(partitionIndex) };
				}
				for (const { opIndex, deleted, partitionIndex } of value.appliedDeletes) {
					results[opIndex] = { outcome: "ok", operation: "delete", deleted, partitionIndex: at(partitionIndex) };
				}
			},
			failure: (opIndex, f, entry) => {
				const error_id = f.fields.error_id ?? defectErrorId("a failed write has no error_id", { opIndex });
				results[opIndex] = { outcome: f.outcome, ...f.fields, code: f.fields.code as FokosErrorCode, error_id };
				if (entry?.reason) {
					conditionFailures.push({ opIndex, reason: entry.reason, itemOmitted: entry.itemOmitted });
				}
			},
		});
		for (const { opIndex, reason: encoded, itemOmitted } of conditionFailures) {
			const result = results[opIndex];
			const reason = withAnswerMeta(meta, () => decodeRejectionReason(encoded));
			if (result?.outcome === "rejected" && reason.code === "condition_failed") {
				result.reason = reason;
				if (itemOmitted) {
					result.itemOmitted = itemOmitted;
				}
			}
		}
		const out = results.map(
			(result, opIndex): BatchWriteItemResult =>
				result ?? {
					outcome: "unknown",
					code: CORE_INTERNAL_CODES.invariant_failed.code,
					error_id: defectErrorId("a batch write answer has no outcome for an item", { opIndex }),
				},
		);
		const unprocessedItems = opts.items.filter((_, i) => out[i].outcome === "unprocessed" || out[i].outcome === "unknown");
		return { results: out, unprocessedItems, meta };
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
